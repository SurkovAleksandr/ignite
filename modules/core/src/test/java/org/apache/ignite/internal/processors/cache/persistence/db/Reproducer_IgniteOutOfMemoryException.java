package org.apache.ignite.internal.processors.cache.persistence.db;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Reproducer_IgniteOutOfMemoryException extends GridCommonAbstractTest {

    private static final String CACHE_NAME_FREE_MEMORY = "MemCache_FreeMemory";
    private static final String CACHE_NAME_1 = "MemCache_1";
    private static final String CACHE_NAME_2 = "MemCache_2";
    private static final String CACHE_GROUP = "GroupMemCache";
    private static final String DATA_REGION_FREE_MEMORY = "dataRegionFreeMemory";
    private static final long DEFAULT_DATA_REGION_SIZE = 10 * 1024 * 1024;


    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            /*new CacheConfiguration(CACHE_NAME_FREE_MEMORY)
                .setDataRegionName(DATA_REGION_FREE_MEMORY),*/
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
                /*.setDataRegionConfigurations(
                    new DataRegionConfiguration()
                        .setName(DATA_REGION_FREE_MEMORY)
                        .setMaxSize(getFreePhysicalMemorySize() - DEFAULT_DATA_REGION_SIZE - 1 * 1024 * 1024)
                )*/
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

    @Test
    public void testDeleteCacheWithSmallDataRegion() throws Exception {

        final IgniteEx igniteEx = startGrid(1);
        igniteEx.cluster().active(true);

        try (final IgniteDataStreamer<Object, Object> streamerFreeMem = igniteEx.dataStreamer(CACHE_NAME_FREE_MEMORY);
            final IgniteDataStreamer<Object, Object> streamer1 = igniteEx.dataStreamer(CACHE_NAME_1);
            final IgniteDataStreamer<Object, Object> streamer2 = igniteEx.dataStreamer(CACHE_NAME_2)) {

            for (int i = 0; i < 100; i++) {
                streamerFreeMem.addData(i, new byte[2001]);
            }

            for (int i = 0; i < 100_000; i++) {
                streamer1.addData(i, "OOM " + i);
                streamer2.addData(i, "OOM2 " + i);
            }
        }

        igniteEx.context().cache().context().database().waitForCheckpoint("OOM end");

        final IgniteCache<Object, Object> cache1 = igniteEx.getOrCreateCache(CACHE_NAME_1);
        cache1.destroy();
    }
}
