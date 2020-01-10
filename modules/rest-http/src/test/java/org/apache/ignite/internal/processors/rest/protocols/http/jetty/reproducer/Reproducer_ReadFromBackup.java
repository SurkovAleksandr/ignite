package org.apache.ignite.internal.processors.rest.protocols.http.jetty.reproducer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Воспроизведение следующей ситуации:
 * стартуют три ноды. Затем создаются потоки, каждый из которых делает вставку в кеш,
 * а зетем пробует получить сохраненное значение.
 * В определенный момен происходит остановка серверного узла.
 * При этом запись(вставка) проходит успешно, но чтение периодически нет.
 *
 * Особенности проведения тестов:
 * - для кэша установить setWriteSynchronizationMode(FULL_SYNC)
 * - для кэша установить setReadFromBackup(false) Эмуляция ситуации, когда один из primary узлов выходит из кластера. При этом put в кэш
 * проходит, а get ничего не получает. Ожидается, что get должен отработать успешно. Исправление сделано в 2.8
 * <p>
 * Из переписки в чате "ignite se + openshift/cuber":
 * - Вопрос: А что происходит в кластере из трех нод, когда запрашиваемый мастер-партишен
 *      находится на первой ноде, а его бекап - на второй. Если мастер упал, а запрос пришел на третью ноду. Какое
 *      поведение в этом случае?
 * - Ответ: Третья нода идет на первую, если еще не знает что она упала, получает отказ,
 *      делает ретрай несколько раз. После того как информация об ушедшей ноде распространяется в кластере, пвторая нода
 *      становится праймари, начинается ребаланс со второй ноды на третью, чтобы осталось заданное количество копий в
 *      кластере. Пока ребаланс не закончен запросы идут на вторую.
 * <p>
 * Есть подозрение вот на этот тикет https://issues.apache.org/jira/browse/IGNITE-10352, он только в 2.8 исправлен,
 * но там поведение немного другое, по ассертам валится.
 */
public class Reproducer_ReadFromBackup extends GridCommonAbstractTest {
    /** Count grids. */
    private static final int countGrids = 3;

    /** Tx cache name. */
    private static final String TX_CACHE = "txCache";

    /** Keys count. */
    private static final int KEYS_CNT = 1000;

    /** todo Stop load flag. */
    private static final AtomicBoolean stop = new AtomicBoolean();

    /** todo Error. */
    private static final AtomicReference<Throwable> err = new AtomicReference<>();

    private static final String CLIENT_PREFIX = "client";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        //todo
        /*cfg.setFailureHandler(new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                err.compareAndSet(null, failureCtx.error());
                stop.set(true);
                return false;
            }
        });*/

        if (igniteInstanceName.contains(CLIENT_PREFIX)) {
            cfg.setClientMode(true);
        }

        CacheConfiguration<Long, Long> txCcfg = new CacheConfiguration<Long, Long>(TX_CACHE)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(false);

        cfg.setCacheConfiguration(txCcfg);
        return cfg;
    }

    /**
     * Тест воспроизводит ситуацию, когда вставка и получение значения из/в кэш осуществляется через серверные узлы.
     * */
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

                try {
                    cache0.put(keyValue, keyValue);

                    Long valueCache1 = cache1.get(keyValue);
                    Long valueCache2 = cache2.get(keyValue);
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
        });

        //grid0.close();
        Ignition.stop(grid0.name(), false);

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

    /**
     * Тест воспроизводит ситуацию, когда вставка и получение значения из/в кэш осуществляется через клиентские узлы.
     * */
    public void testPutAndGetWroughtClient() throws Exception {
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
                //Увеличение номера ключа делается для того, чтобы каждый клиент работал со своим ключем
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

    /**
     * Тест воспроизводит ситуацию, когда вставка и получение значения из/в кэш осуществляется посредством http запросов(Rest API).
     *
     * Для работы теста надо:
     * - чтобы в classpath был ignite-rest-http
     * - установить коннектор cfg.setConnectorConfiguration(new ConnectorConfiguration());
     */
    public void testPutAndGetWroughtRestApi() throws Exception {
        final IgniteEx grid0 = startGrids(countGrids);

        grid0.cluster().active(true);

        List<String> nodeIds = grid0.cluster().nodes().stream()
            .map(node -> node.id().toString())
            .collect(Collectors.toList());

        final IgniteCache<Long, Long> cache0 = grid0.cache(TX_CACHE);

        final List<Integer> g0Keys = primaryKeys(cache0, KEYS_CNT);

        AtomicBoolean isSuccessTest = new AtomicBoolean(true);

        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (Integer key : g0Keys) {
            final Long keyValue = Long.valueOf(key);

            executorService.submit(() -> {
                try {
                    if (!isSuccessTest.get()) {
                        return;
                    }
                    //destId
                    doRequest("cmd=put&key=" + keyValue + "&val=" + keyValue + "&cacheName=" + TX_CACHE + "&destId=" + nodeIds.get(0));

                    /*"successStatus" -> {Integer@6805} 0
                    "affinityNodeId" -> "8b865fb4-2a8c-4826-a158-d7402e600000"
                    "error" -> null
                    "response" -> "3"
                    "sessionToken" -> null*/
                    final Object obj1 = doRequest("cmd=get&key=" + keyValue + "&cacheName=" + TX_CACHE + "&destId=" + nodeIds.get(1)).get("response");
                    final Long response1 = Long.valueOf(obj1 == null ? "-1" : obj1.toString());
                    final Object obj2 = doRequest("cmd=get&key=" + keyValue + "&cacheName=" + TX_CACHE + "&destId=" + nodeIds.get(2)).get("response");
                    final Long response2 = Long.valueOf(obj2 == null ? "-2" : obj2.toString());

                    if (!(keyValue.equals(response1) && response1.equals(response2))) {
                        isSuccessTest.set(false);
                        log.error("Error getting value " + keyValue + ". valueCache1=" + response1 + ", valueCache2=" + response2);
                    }
                }
                catch (IllegalStateException | IOException ex) {
                    if(!(ex instanceof ConnectException)) {
                        log.error("Happened some error", ex);
                    }
                }
            });
        }

        TimeUnit.MILLISECONDS.sleep(500);
        grid0.close();
        //Ignition.stop(grid0.name(), false);

        GridTestUtils.waitForCondition((PA)() -> !isSuccessTest.get(), 20000);

        assertTrue(isSuccessTest.get());
    }

    private Map<String, Object> doRequest(String params) throws IOException {
        final String baseUrl = "http://localhost:8080/ignite?";
        String requestUrl = baseUrl + params;

        URLConnection conn = new URL(requestUrl).openConnection();

        conn.connect();

        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> responseMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            return responseMap;
        }
    }
}