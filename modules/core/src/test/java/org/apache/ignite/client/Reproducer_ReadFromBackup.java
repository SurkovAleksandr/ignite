package org.apache.ignite.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Воспроизведение следующей ситуации: стартуют три ноды идет вставка в кеш. Потом чтение. При этом запись проходит
 * успешно, но чтение периодически нет.
 */
public class Reproducer_ReadFromBackup extends GridCommonAbstractTest {
    /**
     * Count grids
     */
    private static final int countGrids = 3;

    /**
     * Tx cache name.
     */
    private static final String TX_CACHE = "txCache";

    /**
     * Atomic cache name.
     */
    private static final String ATOMIC_CACHE = "atomicCache";

    /**
     * Keys count.
     */
    private static final int KEYS_CNT = 50000;

    /**
     * Stop load flag.
     */
    private static final AtomicBoolean stop = new AtomicBoolean();

    /**
     * Error.
     */
    private static final AtomicReference<Throwable> err = new AtomicReference<>();

    private static final String CLIENT_PREFIX = "client";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        /*cfg.setFailureHandler(new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                err.compareAndSet(null, failureCtx.error());
                stop.set(true);
                return false;
            }
        });*/

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration<Long, Long> txCcfg = new CacheConfiguration<Long, Long>(TX_CACHE)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(false);

        /*CacheConfiguration<Long, Long> atomicCcfg = new CacheConfiguration<Long, Long>(ATOMIC_CACHE)
            .setAtomicityMode(ATOMIC)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(true);*/

        cfg.setCacheConfiguration(txCcfg/*, atomicCcfg*/);
        return cfg;
    }

    /**
     * Исходные данные: - для кэша установить setWriteSynchronizationMode(FULL_SYNC) - для кэша установить
     * setReadFromBackup(false) Эмуляция ситуации, когда один из primary узлов выходит из кластера. При этом put в кэш
     * проходит, а get ничего не получает. Ожидается, что get должен отработать успешно. Исправление сделано в 2.8
     * <p>
     * Из переписки в чате: Вопрос: А что происходит в кластере из трех нод, когда запрашиваемый мастер-партишен
     * находится на первой ноде, а его бекап - на второй. Если мастер упал, а запрос пришел на третью ноду. Какое
     * поведение в этом случае? Ответ: Третья нода идет на первую, если еще не знает что она упала, получает отказ,
     * делает ретрай несколько раз. После того как информация об ушедшей ноде распространяется в кластере, пвторая нода
     * становится праймари, начинается ребаланс со второй ноды на третью, чтобы осталось заданное количество копий в
     * кластере. Пока ребаланс не закончен запросы идут на вторую.
     * <p>
     * Есть подозрение вот на этот тикет https://issues.apache.org/jira/browse/IGNITE-10352, он только в 2.8 исправлен,
     * но там поведение немного другое, по ассертам валится.
     */
    @Test
    //@WithSystemProperty(key = IGNITE_NO_SHUTDOWN_HOOK, value = "true")
    public void testPutAndGetValue() throws Exception {
        final IgniteEx grid0 = startGrids(countGrids);
        grid0.cluster().active(true);

        final IgniteCache<Long, Long> cache0 = grid0.cache(TX_CACHE);
        final IgniteCache<Long, Long> cache1 = grid(1).cache(TX_CACHE);
        final IgniteCache<Long, Long> cache2 = grid(2).cache(TX_CACHE);

        final List<Integer> g0Keys = primaryKeys(cache0, 10_000);

        List<Future<Boolean>> futureList = new ArrayList<>(g0Keys.size());
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        g0Keys.forEach(integer -> {
            futureList.add(executorService.submit(() -> {
                Long keyValue = Long.valueOf(integer);

                boolean isSuccess = true;

                /*try {*/
                    cache0.put(keyValue, keyValue);

                    Long valueCache1 = cache1.get(keyValue);
                    Long valueCache2 = cache2.get(keyValue);
                    isSuccess = keyValue.equals(valueCache1) && valueCache1.equals(valueCache2);
                    if (!isSuccess) {
                        log.error("Error getting value " + keyValue + ". valueCache1=" + valueCache1 + ", valueCache2=" + valueCache2);
                    }
                    return isSuccess;
                /*} catch (IllegalStateException ex) {
                    isSuccess = ex.getCause() instanceof CacheStoppedException;
                }*/

                //return isSuccess;
            }));
        });

        grid0.close();
        //Ignition.stop(grid0.name(), false);

        for (Future<Boolean> future : futureList) {
            Boolean isSuccess = true;
            try {
                isSuccess = future.get(10, TimeUnit.SECONDS);
            }
            catch (ExecutionException ex) {
                log.error("Get was failed " + ex.getMessage());
            }
            assertTrue(isSuccess);
        }
    }

    @Test
    public void testPutAndGetThroughClient() throws Exception {
        final IgniteEx grid0 = startGrids(countGrids);
        grid0.cluster().active(true);

        List<IgniteEx> clientList = new ArrayList<>();
        clientList.add(startGrid(CLIENT_PREFIX + "0"));
        clientList.add(startGrid(CLIENT_PREFIX + "1"));
        clientList.add(startGrid(CLIENT_PREFIX + "2"));

        final IgniteCache<Long, Long> cache0 = grid0.cache(TX_CACHE);

        final List<Integer> g0Keys = primaryKeys(cache0, 10_000);

        List<Future<Boolean>> futureList = new ArrayList<>(g0Keys.size());
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < g0Keys.size(); i++) {
            for (IgniteEx client : clientList) {
                final Long keyValue = Long.valueOf(g0Keys.get(i));
                i++;
                futureList.add(executorService.submit(() -> {
                    boolean isSuccess = true;

                    try {
                        IgniteCache<Long, Long> cache = client.getOrCreateCache(TX_CACHE);
                        cache.put(keyValue, keyValue);

                        Long valueCache1 = cache.get(keyValue);
                        Long valueCache2 = cache.get(keyValue);
                        isSuccess = keyValue.equals(valueCache1) && valueCache1.equals(valueCache2);
                        if (!isSuccess) {
                            log.error("Error getting value " + keyValue + ". valueCache1=" + valueCache1 + ", valueCache2=" + valueCache2);
                        }
                        return isSuccess;
                    }
                    catch (IllegalStateException ex) {
                        isSuccess = ex.getCause() instanceof CacheStoppedException;
                    }

                    return isSuccess;
                }));
            }
        }
        U.sleep(100);
        grid0.close();
        //Ignition.stop(grid0.name(), false);

        for (Future<Boolean> future : futureList) {
            Boolean isSuccess = true;
            try {
                isSuccess = future.get(10, TimeUnit.SECONDS);
            }
            catch (ExecutionException ex) {
                log.error("Get was failed " + ex.getMessage());
            }
            assertTrue(isSuccess);
        }
    }
}

