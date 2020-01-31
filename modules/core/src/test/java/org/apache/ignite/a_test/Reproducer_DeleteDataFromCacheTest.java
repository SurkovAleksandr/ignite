package org.apache.ignite.a_test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Reproducer_DeleteDataFromCacheTest extends GridCommonAbstractTest {
    static final String CACHE_NAME_1 = "cache_1";
    static final String CACHE_NAME_2 = "cache_2";
    static final String CACHE_GROUP = "cacheGroup";

    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if ("client".equals(igniteInstanceName)) {
            cfg.setClientMode(true);
        }

        final CacheConfiguration<Long, String> cacheCfg1 = new CacheConfiguration<>();
        cacheCfg1
            .setName(CACHE_NAME_1)
            .setGroupName(CACHE_GROUP);

        final CacheConfiguration<Long, String> cacheCfg2 = new CacheConfiguration<>();
        cacheCfg2
            .setName(CACHE_NAME_2)
            .setGroupName(CACHE_GROUP);

        cfg
            .setCacheConfiguration(cacheCfg1, cacheCfg2);
        cfg
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );

        return cfg;
    }

    /**
     * На каждый узел отправляется задача на удаление данныз изкэша. Узел удаляет данные только из primary партицый.
     * После удаления данных каждый узел отправляет сообщение клиенту.
     * <p>
     * --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
     */
    @Test
    public void testMessageFromBroadcust() throws Exception {

        int countEntry = 1000;

        final IgniteEx grid0 = (IgniteEx)startGrids(2);
        grid0.cluster().active(true);
        final IgniteEx client = startGrid("client");

        final ThreadLocalRandom random = ThreadLocalRandom.current();

        System.out.println(">>>>>> Size cache before: " + client.cache(CACHE_NAME_1).size());

        try (final IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(CACHE_NAME_1)) {
            for (int i = 0; i < countEntry; i += 2) {
//                streamer.addData(i, new Person((long)i, "Name_" + i, random.nextInt(0, 11)));
                streamer.addData(i, new Person((long)i, "Name_" + i, 5));
//                streamer.addData(i, new Person((long)i, "Name_" + i, random.nextInt(11, 40)));
                streamer.addData(i + 1, new Person((long)i, "Name_" + i, 15));
            }

            /*for (int i = 0; i < countEntry/2; i++) {
                streamer.addData(i, new Person((long)i, "Name_" + i, random.nextInt(0, 11)));
            }

            for (int i = 0; i < countEntry/2; i++) {
                streamer.addData(i, new Person((long)i, "Name_" + i, random.nextInt(11, 40)));
            }*/
        }
        catch (IllegalStateException e) {
            e.printStackTrace();
        }

        grid0.context().cache().context().database().waitForCheckpoint("End running dataStreamer");

        assertEquals(countEntry, grid0.cache(CACHE_NAME_1).size());

        final long start = System.currentTimeMillis();

        final Collection<String> resultCollection = client.compute().broadcast(new DeleteDataCallable());

        final long end = System.currentTimeMillis();

        grid0.context().cache().context().database().waitForCheckpoint("End running dataStreamer");
        System.out.println(">>>>>> Duration of deleting: " + (end - start));
        System.out.println(">>>>>> Size cache after: " + client.cache(CACHE_NAME_1).size());

        resultCollection.forEach(System.out::println);

        assertEquals(countEntry/2, client.cache(CACHE_NAME_1).size());
    }

    @Test
    public void testGenerate() throws Exception {
        final IgniteEx grid0 = (IgniteEx)startGrids(2);
        grid0.cluster().active(true);
        int countEntry = 10_000_000;

        System.out.println(">>>>>> Size cache before: " + grid0.cache(CACHE_NAME_1).size());

        List<Integer> list1 = new ArrayList<>(countEntry / 2);
        List<Integer> list2 = new ArrayList<>(countEntry / 2);
        try (final IgniteDataStreamer<Object, Object> streamer = grid0.dataStreamer(CACHE_NAME_1)) {
            for (int i = 0; i < countEntry; i += 2) {
                streamer.addData(i, new Person((long)i, "Name_" + i, 5));
                list1.add(i);

                streamer.addData(i + 1, new Person((long)i, "Name_" + i, 15));
                list2.add(i);
            }
        }
        catch (IllegalStateException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();

        grid0.compute().broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            private IgniteEx ignite;
            @LoggerResource
            private IgniteLogger log;

            @Override public void run() {
                final int[] primaryPartitions = ignite.affinity(CACHE_NAME_1).primaryPartitions(ignite.localNode());

                final IgniteInternalCache<Object, Object> cache = ignite.cachex(CACHE_NAME_1).keepBinary();

                for (int partitionId : primaryPartitions) {

                    final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                    List<Future<Long>> futureList = new ArrayList<>();

                    futureList.add(executorService.submit(() -> {
                        final GridCacheContext<Object, Object> context = cache.context();
                        final GridIterator<CacheDataRow> partitionIterator = context.offheap().cachePartitionIterator(context.cacheId(), partitionId);
                        List<KeyCacheObject> keysForDelete = new ArrayList<>();
                        while (partitionIterator.hasNext()) {
                            CacheDataRow row = partitionIterator.next();

                            final BinaryObject value = (BinaryObject)row.value();

                            if ((int)value.field("age") > 10) {
                                keysForDelete.add(row.key());
                            }

                                /*if (keysForDelete.size() == 100) {
                                    try {
                                        cache.removeAll(keysForDelete);
                                    }
                                    catch (IgniteCheckedException e) {
                                        e.printStackTrace();
                                    }

                                }*/
                        }

                        try {
                            cache.removeAll(keysForDelete);
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }

                        return (long)keysForDelete.size();
                    }));

                    futureList.forEach(future -> {
                        try {
                            future.get(10, TimeUnit.SECONDS);
                        }
                        catch (InterruptedException | ExecutionException | TimeoutException e) {
                            e.printStackTrace();
                        }
                    });

                }

            }
        });

        long end = System.currentTimeMillis();

        log.info(">>>>>> Duration of delete data: " + (end - start));

        int counter1 = 0;
        int counter2 = 0;

        final IgniteCache<Long, Person> cache = grid0.cache(CACHE_NAME_1);
        final Iterator<Cache.Entry<Long, Person>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            final Cache.Entry<Long, Person> entry = iterator.next();
            if (entry.getValue().getAge() > 10) {
                counter2++;
            }
            else {
                counter1++;
            }
        }

        System.out.println(">>>>>> counter: " + counter1 + " - " + counter2);
        System.out.println(">>>>>> Size cache after: " + grid0.cache(CACHE_NAME_1).size());

        assertEquals(countEntry / 2, grid0.cache(CACHE_NAME_1).size());
    }
}
