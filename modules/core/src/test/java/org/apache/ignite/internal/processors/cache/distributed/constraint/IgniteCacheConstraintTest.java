package org.apache.ignite.internal.processors.cache.distributed.constraint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.NearTxFinishFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.TypePartitionForKey;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Тесты к задаче IGN-115.
 * <p>
 * Для задержки отправки сообщений использовать {@link TestRecordingCommunicationSpi}. Пример использования {@link CacheMvccTxRecoveryTest}
 * <p>
 * Возможно тесты из этой задачи помогут https://issues.apache.org/jira/browse/IGNITE-5935
 */
public class IgniteCacheConstraintTest extends GridCommonAbstractTest {
    private static final int SRVS = 2;
    private boolean client;
    private boolean clientDiscovery;
    private IgniteEx igniteEx;
    //Идентификаторы узлов на которых будут блокироваться сообщения.
    private List<UUID> nodeForBlockMessage = new ArrayList<>();
    private static final String CACHE_NAME_A = "Cache_A";
    private static final String CACHE_NAME_B = "Cache_B";
    /**
     * Начальное значение счета в объектах кэша {@link #CACHE_NAME_A}. Начальной значение счета в кэше {@link
     * #CACHE_NAME_B} будет равно 0. Т.е. перевод осуществляется с объекта в кэше {@link #CACHE_NAME_A} в объект в кэше
     * {@link #CACHE_NAME_B}.
     */
    private static final int BEGIN_ACCOUNT = 1000;
    //Количествое объектов счетов в каждом кэше
    private static final int COUNT_ACCOUNTS = 1000;
    //Количество переводов средст с одного счета на другой.
    // В рамках теста с одного счета снимается 1, а на другой добавляется 1.
    private static final int COUNT_TRANSFERS = 10;

    private volatile boolean isRunTest = true;

    /*IgniteCache<Long, Long> cacheA = ignite.cache(CACHE_NAME_A);
    IgniteCache<Long, Long> cacheB = ignite.cache(CACHE_NAME_B);*/

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        igniteEx = startGrids(SRVS);
        igniteEx.cluster().active(true);
        final Iterator<ClusterNode> iterator = igniteEx.cluster().nodes().iterator();
        for (int i = 0; i < 2; i++) {
            nodeForBlockMessage.add(iterator.next().id());
        }

        //todo G.allGrids();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /*@Override protected boolean isMultiJvm() {
        return true;
    }*/

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");
        cfg.setFailureDetectionTimeout(20_000);

        cfg.setConnectorConfiguration(null);
        //cfg.setPeerClassLoadingEnabled(false);
        cfg.setTimeServerPortRange(200);

        MemoryEventStorageSpi eventSpi = new MemoryEventStorageSpi();
        eventSpi.setExpireCount(100);

        cfg.setEventStorageSpi(eventSpi);

        //Установка тестового CommunicationSpi для блокировки сообщений
        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        spi.setLocalPortRange(200);
        spi.setSharedMemoryPort(-1);
        cfg.setCommunicationSpi(spi);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinderCleanFrequency(10 * 60_000);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(2 * 60_000);

        if (!clientDiscovery)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setClientMode(client);

        cfg.setCacheConfiguration(createCacheConfig(CACHE_NAME_A), createCacheConfig(CACHE_NAME_B));

        return cfg;
    }

    private CacheConfiguration<Long, Long> createCacheConfig(String cacheName) {
        CacheConfiguration<Long, Long> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * Получение пары списков ключей для двух кэшей так, чтобы они мапились на разные узлы.
     */
    @Test
    public void gettingKeyWithDifferentCacheAndNode () {
        final IgniteEx server0 = grid(0);
        final IgniteEx server1 = grid(1);

        final IgniteCache<Long, AccountB> cacheA = server0.cache(CACHE_NAME_A);
        final IgniteCache<Long, AccountB> cacheB = server0.cache(CACHE_NAME_B);

        int cntKeys = 100;
        final Set<Integer> integerSet0 = new HashSet<>(findKeys(server0.localNode(), cacheA, cntKeys, 0, TypePartitionForKey.PRIMARY));
        final Set<Integer> integerSet1 = new HashSet<>(findKeys(server1.localNode(), cacheB, cntKeys, 0, TypePartitionForKey.PRIMARY));

        assertTrue(integerList0.size() > 0);
        assertTrue(integerList1.size() > 0);
        assertEquals(integerList1.size(), integerList1.size());

        final Set<Integer> setKeys = Stream.concat(integerList0.stream(), integerList1.stream())
            .collect(Collectors.toSet());

        assertEquals(cntKeys * 2, setKeys.size());
    }

    @Test
    public void blockTransactionMessage() throws Exception {
        client = true;

        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future> futureList = new ArrayList<>();

        final IgniteEx ignite0 = ignite(0);
        //todo посмотреть primaryKeys()

        for (int i = 0; i < 1; i++) {
            int clientNumber = i;

            if(!isRunTest) break;

            futureList.add(executorService.submit(() -> {
                Thread.currentThread().setName("ClientThread" + clientNumber);
                try {
                    String clientName = "MyClient" + clientNumber;
                    final IgniteEx client = startGrid(getConfiguration(clientName));
                    /*NearTxFinishFuture*/

                    TestRecordingCommunicationSpi.spi(client).blockMessages((node, message) -> {
                        log.info("Block message: " + message + " : " + node);

                        if(nodeForBlockMessage.contains(node.id()) &&
                            (message instanceof GridDhtTxPrepareRequest ||
                             message instanceof GridNearTxPrepareRequest)) {//С клиента НАВЕРНО приходит GridNearTxPrepareRequest

                            new Thread(() -> {
                                IgniteProcessProxy.kill(clientName);

                                log.info("Process killed");
                                log.info("Client name: " + client.name());
                            }).start();

                            return false;
                        }

                        return false;
                    });

                    addBatchAccounts(client, clientNumber);
                }
                catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // Т.к. произошла ошибка, то останавливаем остальные потоки
                    isRunTest = false;
                    stopExecutorService(executorService);
                }
            }));
        }

        for (Future future : futureList) {
            while (true) {
                if(!isRunTest) break;

                try {
                    future.get(10, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {

                }
            }
        }
    }

    private void addBatchAccounts(IgniteEx client, int clientNumber) {
        boolean isTransactionSuccess = false;
        int rangeOfBatch = 10;//должен быть не больше количества вставляемых элементов countIteration
        int countIteration = 1;
        long beginRange = clientNumber * rangeOfBatch;

        IgniteCache<Long, AccountA> cacheA = client.cache(CACHE_NAME_A);
        IgniteCache<Long, AccountB> cacheB = client.cache(CACHE_NAME_B);

        try (Transaction transaction = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {

            for (int i = 0; i < countIteration; i++) {
                long objectId = beginRange + countIteration;
                cacheA.put(objectId, new AccountA(objectId, "A_name_" + objectId, BEGIN_ACCOUNT));
                cacheB.putIfAbsent(objectId, new AccountB(objectId, "B_name_" + objectId, 0));
            }

            transaction.commit();
            isTransactionSuccess = true;
        }
        catch (Exception e) {
            log.error("Some error in transaction", e);
        }

        if (isTransactionSuccess) {
            assertEquals("Not correct values for client " + clientNumber, cacheA.size(CachePeekMode.PRIMARY), cacheB.size(CachePeekMode.PRIMARY));
        } else {
            log.error("Transaction for client '" + clientNumber + "' finished with error");
        }

        log.info("Number of grid node: " + client.cluster().nodes().size());
        log.info("Number of grid node: " + G.allGrids().size());//todo всегда возвращает 1

        int countElementInCacheA = 0;
        int countElementInCacheB = 0;
        for (long i = beginRange; i < beginRange + countIteration; i++) {
            if (cacheA.containsKey(beginRange)) {
                countElementInCacheA++;
            }

            if (cacheB.containsKey(beginRange)) {
                countElementInCacheB++;
            }
        }

        assertEquals(countElementInCacheA, countElementInCacheB);
    }

    private synchronized void stopExecutorService(ExecutorService executor) {
        try {
            /*Добавление новых потоков приостанавливается и ожидается завершение текущих*/
            executor.shutdown();
            /*Если потоки не завершаться через указанное время, то завершить их*/
            executor.awaitTermination(6, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("termination interrupted");
        } finally {
            if (!executor.isTerminated()) {
                System.err.println("killing non-finished tasks");
            }
            executor.shutdownNow();
        }
    }

    /**
     * Воспроизведение ситуации описанной в задаче IGN-115 на альфе. С клиента в одной транзакции выполняется изменение
     * двух кэшей. Не дожидаясь завершения транзакции останавливается клиен. Ожидается, что кэши должны иметь
     * согласованные значения(в IGN-115 это не так).
     * <p>
     * Особенности: - должна быть большая нагрузка - кэши находились на разных узлах
     */
    @Test
    public void twoCacheConstraintTest() throws Exception {
        client = true;

        try (IgniteEx ignite = startGrid(SRVS)) {
            log.info("---> Start client for populate data");
            IgniteCache<Long, AccountA> cacheA = ignite.cache(CACHE_NAME_A);
            IgniteCache<Long, AccountB> cacheB = ignite.cache(CACHE_NAME_B);
            populateCaches(cacheA, cacheB);
            checkCachesOnPartitions(ignite, cacheA, cacheB);
            log.info("---> End client for populate data");
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<Future> clientThreads = new ArrayList<>(COUNT_ACCOUNTS);
        log.info("---> Start transfer data");
        for (int i = 0; i < COUNT_ACCOUNTS; i++) {
            Long keyIndex = (long)i;
            clientThreads.add(executorService.submit(() -> createClientAndRunTransaction(keyIndex)));
        }
        log.info("---> Ent transfer data");

        log.info("---> Start waiting client");
        for (Future thread : clientThreads) {
            thread.get();
        }
        log.info("---> End waiting client");

        checkResultOfTransferTest();
    }

    private void checkResultOfTransferTest() throws Exception {
        try (IgniteEx ignite = startGrid("CheckResultOfTest")) {
            assertTrue(ignite.configuration().isClientMode());

            IgniteCache<Long, AccountA> cacheA = ignite.cache(CACHE_NAME_A);
            IgniteCache<Long, AccountB> cacheB = ignite.cache(CACHE_NAME_B);

            log.info("---> Start check cache A");
            final Iterator<Cache.Entry<Long, AccountA>> iteratorA = cacheA.iterator();
            while (iteratorA.hasNext()) {
                final Cache.Entry<Long, AccountA> entry = iteratorA.next();

                assertEquals("Wrong value for account(cache " + CACHE_NAME_A + "): " + entry.getValue(), BEGIN_ACCOUNT - COUNT_TRANSFERS, entry.getValue().account);
            }
            log.info("---> End check cache A");

            log.info("---> Start check cache B");
            final Iterator<Cache.Entry<Long, AccountB>> iteratorB = cacheB.iterator();
            while (iteratorB.hasNext()) {
                final Cache.Entry<Long, AccountB> entry = iteratorB.next();
                assertEquals("Wrong value for account(cache " + CACHE_NAME_B + "): " + entry.getValue(), COUNT_TRANSFERS, entry.getValue().account);
            }
            log.info("---> End check cache B");
        }
    }

    private void createClientAndRunTransaction(Long keyIndex) {
        try (IgniteEx ignite = startGrid(SRVS + "Client" + keyIndex)) {
            assertTrue(ignite.configuration().isClientMode());

            IgniteCache<Long, AccountA> cacheA = ignite.cache(CACHE_NAME_A);
            IgniteCache<Long, AccountB> cacheB = ignite.cache(CACHE_NAME_B);

            for (int i = 0; i < COUNT_TRANSFERS; i++) {
                try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                    AccountA valueA = cacheA.get(keyIndex);
                    AccountB valueB = cacheB.get(keyIndex);

                    valueA.account--;
                    valueB.account++;
                    cacheA.put(keyIndex, valueA);

                    //TimeUnit.MILLISECONDS.sleep(100);

                    cacheB.put(keyIndex, valueB);
                    transaction.commit();
                }
                catch (Exception e) {
                    log.error("Error transfer value: ", e);
                    return;
                }
            }
        }
        catch (Exception e) {
            log.error("Error run transfer", e);
            return;
        }
    }

    /**
     * Наполнение кэшей данными.
     */
    private void populateCaches(
        IgniteCache<Long, AccountA> cacheA,
        IgniteCache<Long, AccountB> cacheB) {

        List<Future> futureList = new ArrayList<>();

        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final Future<?> submit = executorService.submit(() -> {
                for (int i1 = 0; i1 < COUNT_ACCOUNTS; i1++) {
                    cacheA.put((long)i1, new AccountA(i1, "A_name_" + i1, BEGIN_ACCOUNT));
                    cacheB.put((long)i1, new AccountB(i1, "B_name_" + i1, 0));
                }
            });
            futureList.add(submit);
        }

        for (Future future : futureList) {
            try {
                future.get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private void checkCachesOnPartitions(IgniteEx ignite,
        IgniteCache<Long, AccountA> cacheA,
        IgniteCache<Long, AccountB> cacheB) {

        int theSame = 0;
        for (long i = 0; i < COUNT_ACCOUNTS; i++) {
            final AccountA valueA = cacheA.get(i);
            final AccountB valueB = cacheB.get(i);

            final Affinity<Object> affinityA = ignite.affinity(CACHE_NAME_A);
            int partA = affinityA.partition(valueA);
            final Affinity<Object> affinityB = ignite.affinity(CACHE_NAME_B);
            int partB = affinityB.partition(valueB);

            if (affinityA.mapPartitionToNode(partA).id().equals(affinityB.mapPartitionToNode(partB).id())) {
                theSame++;
            }
        }

        System.out.println("The same node: " + theSame);
    }

    @Test
    public void batchAddToCaches() {
        client = true;
        int countThread = 1000;

        try (IgniteEx ignite = startGrid("ClientBatchAddToCaches")) {
            IgniteCache<Long, AccountA> cacheA = ignite.cache(CACHE_NAME_A);
            IgniteCache<Long, AccountB> cacheB = ignite.cache(CACHE_NAME_B);

            List<Future> futureList = new ArrayList<>();
            final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            for (int k = 0; k < countThread; k++) {
                //Далее n*COUNT_ACCOUNTS + i означает, что каждый поток вставляет данные только в свой диапозон
                long n = k;
                futureList.add(executorService.submit(() -> {
                    long mult = n * COUNT_ACCOUNTS;
                    for (int i = 0; i < COUNT_ACCOUNTS; i++) {
                        long index = mult + i;
                        try (Transaction transaction = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {

                            cacheA.put(index, new AccountA(index, "Name_A", 100));
                            cacheB.put(index, new AccountB(index, "Name_B", 100));

                            transaction.commit();
                        }
                    }
                }));

            }

            for (Future future : futureList) {
                future.get();
            }
            executorService.shutdown();

            assertEquals(countThread * COUNT_ACCOUNTS, cacheB.size(CachePeekMode.PRIMARY));
            assertEquals(cacheA.size(CachePeekMode.PRIMARY), cacheB.size(CachePeekMode.PRIMARY));

            log.info("Size " + CACHE_NAME_A + ": " + cacheA.size(CachePeekMode.PRIMARY) + ": " + cacheA.size(CachePeekMode.ALL));
            log.info("Size " + CACHE_NAME_B + ": " + cacheB.size(CachePeekMode.PRIMARY) + ": " + cacheB.size(CachePeekMode.ALL));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
