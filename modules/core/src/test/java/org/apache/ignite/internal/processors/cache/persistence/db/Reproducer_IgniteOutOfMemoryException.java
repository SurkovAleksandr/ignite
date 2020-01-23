package org.apache.ignite.internal.processors.cache.persistence.db;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Reproducer_IgniteOutOfMemoryException extends GridCommonAbstractTest {
    private static final String CACHE_NAME_1 = "MemCache_1";
    private static final String CACHE_NAME_2 = "MemCache_2";
    private static final String CACHE_GROUP = "GroupMemCache";
    private static final long DEFAULT_DATA_REGION_SIZE = 10 * 1024 * 1024;


    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

       /**  config for {@link #testDeleteCacheWithForLocalCacheMode()}
       cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME_1)
                .setGroupName(CACHE_GROUP)
                .setCacheMode(CacheMode.LOCAL),
            new CacheConfiguration(CACHE_NAME_2)
                .setGroupName(CACHE_GROUP)
                .setCacheMode(CacheMode.LOCAL)
        );

       cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DEFAULT_DATA_REGION_SIZE)
                )
       );*/


        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(256L * 1024 * 1024)
            ));

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME).setGroupName("grp"),
            new CacheConfiguration("another_cache").setGroupName("grp")
        );

        return cfg;
    }

    private long getFreePhysicalMemorySize() {
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();

        return os.getFreePhysicalMemorySize();
    }

    /**
     * Воспроизведение IgniteOutOfMemoryException на кэшах с CacheMode.LOCAL.
     * */
    @Test
    public void testDeleteCacheWithForLocalCacheMode() throws Exception {
        final IgniteEx igniteEx = startGrid(1);
        igniteEx.cluster().active(true);

        try (final IgniteDataStreamer<Object, Object> streamer1 = igniteEx.dataStreamer(CACHE_NAME_1);
             final IgniteDataStreamer<Object, Object> streamer2 = igniteEx.dataStreamer(CACHE_NAME_2)) {

            final long memorySize = getFreePhysicalMemorySize();
            long iteration = memorySize / 4096;

            for (int i = 0; i < 100; i++) {
                streamer1.addData(i, new byte[2001]);
            }


            for (int i = 0; i <= iteration; i++) {
                streamer2.addData(i, new byte[2001]);
            }
        }

        igniteEx.context().cache().context().database().waitForCheckpoint("OOM end");

        final IgniteCache<Object, Object> cache1 = igniteEx.getOrCreateCache(CACHE_NAME_1);
        cache1.destroy();
    }

    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    @Test
    public void testDestroyCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            PageMemoryEx pageMemory = (PageMemoryEx)ignite.cachex(DEFAULT_CACHE_NAME).context().dataRegion().pageMemory();

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                streamer.addData(i, new byte[pageMemory.pageSize() / 2]);
        }

        ignite.destroyCache(DEFAULT_CACHE_NAME);
    }
}
