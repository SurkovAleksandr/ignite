package org.apache.ignite.internal.processors.cache.my_sandbox;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Пример взят из вопроса Дащинского Вани
 * Ай нид хелп -- этот тест у меня на мастере из 30 5 раз валится со странной фигней
 * Если это ожидаемое поведение, то плиз объясните почему.
 *
 * Caused by: java.lang.AssertionError: Invalid version for inner update [isNew=false, entry=GridDhtCacheEntry [rdrs=ReaderId[] [], part=100, super=GridDistributedCacheEntry [super=GridCacheMapEntry [key=KeyCacheObjectImpl [part=100, val=100, hasValBytes=true], val=PointOfInterest [idHash=1265448417, hash=1492433019, name=POI_100, latitude=null, longitude=null, NAME=POI_100], ver=GridCacheVersion [topVer=203354504, order=1591874500062, nodeOrder=1], hash=100, extras=null, flags=0]]], newVer=GridCacheVersion [topVer=203354503, order=1591874501171, nodeOrder=1]]
 * 			at org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure.versionCheck(GridCacheMapEntry.java:6703)
 * 			at org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure.call(GridCacheMapEntry.java:6090)
 * 			at org.apache.ignite.internal.processors.cache.GridCacheMapEntry$AtomicCacheUpdateClosure.call(GridCacheMapEntry.java:5863)
 * 			at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke.invokeClosure(BPlusTree.java:3994)
 * 			at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree$Invoke.access$5700(BPlusTree.java:3888)
 * 			at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.invokeDown(BPlusTree.java:2020)
 * 			at org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.invoke(BPlusTree.java:1898)
 * 			... 26 more
 *
 * 	Вопрос связан с задачей https://issues.apache.org/jira/browse/IGNITE-12808
 */
public class TestReconnectClientToRestartedCluster extends GridCommonAbstractTest {
    /** */
    public static final String POI_CACHE_NAME = "poi";

    /** */
    public static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    public static final String POI_TABLE_NAME = "POI";

    /** */
    public static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    public static final String ID_FIELD_NAME = "id";

    /** */
    public static final String NAME_FIELD_NAME = "name";

    /** */
    public static final String PK_INDEX_NAME = "_key_pk";

    /** */
    public static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    public static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    public static final int NUM_ENTRIES = 500;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setConsistentId(gridName);

        cfg.setSqlSchemas(POI_SCHEMA_NAME);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }


    /** */
    @Test
    public void testReconnectClient_RestartFromCoordinator() throws Exception {
        testReconnectClient(true);
    }

    /**
     * @param startFromCrd If @{code true}, start grid from old coordinator.
     *
     * @throws Exception if failed.
     */
    private void testReconnectClient(boolean startFromCrd) throws Exception {
        IgniteEx srv0 = startGrids(2);

        IgniteEx cli = startClientGrid(2);

        cli.cluster().state(ClusterState.ACTIVE);

        cli.context().query().querySqlFields(new SqlFieldsQuery(
            String.format("CREATE TABLE %s.%s " +
                    "(%s INT, %s VARCHAR," +
                    " %s DOUBLE PRECISION," +
                    " %s DOUBLE PRECISION," +
                    " PRIMARY KEY (%s)" +
                    ") WITH " +
                    " \"CACHE_NAME=%s,VALUE_TYPE=%s,TEMPLATE=REPLICATED\"",
                POI_SCHEMA_NAME, POI_TABLE_NAME, ID_FIELD_NAME, NAME_FIELD_NAME,
                LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME, ID_FIELD_NAME,
                POI_CACHE_NAME, POI_CLASS_NAME)
        ), false);

        fillTestData(srv0);

        stopGrid(1);

        performQueryingIntegrityCheck(srv0);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, cli, srv0, () -> {
            try {
                stopGrid(0);

                if (startFromCrd)
                    startGrid(0);
                else
                    startGrid(1);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to restart cluster", e);
            }
        });

        assertEquals(2, cli.cluster().nodes().size());
        cli.cluster().state(ClusterState.ACTIVE);

        performQueryingIntegrityCheck(cli);
    }

    /** */
    private void performQueryingIntegrityCheck(IgniteEx ig) throws Exception {
        IgniteCache<Object, Object> cache = ig.getOrCreateCache(POI_CACHE_NAME).withKeepBinary();

        List<List<?>> res = cache.query(new SqlFieldsQuery(String.format("SELECT * FROM %s", POI_TABLE_NAME))
            .setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(NUM_ENTRIES, res.size());

        cache.query(new SqlFieldsQuery(
            String.format("DELETE FROM %s WHERE %s = %s", POI_TABLE_NAME, ID_FIELD_NAME,  "100")
        ).setSchema(POI_SCHEMA_NAME)).getAll();

        assertNull(cache.get(100));

        cache.query(new SqlFieldsQuery(
            String.format(
                "INSERT INTO %s(%s) VALUES (%s)",
                POI_TABLE_NAME,
                String.join(",", ID_FIELD_NAME, NAME_FIELD_NAME),
                String.join(",", "100","'test'"))
        ).setSchema(POI_SCHEMA_NAME)).getAll();

        assertNotNull(cache.get(100));

        cache.query(new SqlFieldsQuery(String.format("UPDATE %s SET %s = '%s' WHERE %s = 100",
            POI_TABLE_NAME, NAME_FIELD_NAME, "POI_100", ID_FIELD_NAME)).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals("POI_100", ((BinaryObject)cache.get(100)).field(NAME_FIELD_NAME));

        assertIndexUsed(cache, "SELECT * FROM " + POI_TABLE_NAME + " WHERE " + ID_FIELD_NAME + " = 10", PK_INDEX_NAME);
    }

    /**
     * Fill cache with test data.
     */
    private void fillTestData(Ignite ig) {
        try (IgniteDataStreamer<? super Object, ? super Object> s = ig.dataStreamer(POI_CACHE_NAME)) {
            Random rnd = ThreadLocalRandom.current();

            for (int i = 0; i < NUM_ENTRIES; i++) {
                BinaryObject bo = ig.binary().builder(POI_CLASS_NAME)
                    .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                    .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .build();

                s.addData(i, bo);
            }
        }
    }

    /** */
    private void createTable(IgniteCache<?, ?> cache, String schemaName) {
        cache.query(new SqlFieldsQuery(
            String.format("CREATE TABLE %s.%s " +
                    "(%s INT, %s VARCHAR," +
                    " %s DOUBLE PRECISION," +
                    " %s DOUBLE PRECISION," +
                    " PRIMARY KEY (%s)" +
                    ") WITH " +
                    " \"CACHE_NAME=%s,VALUE_TYPE=%s\"",
                schemaName, POI_TABLE_NAME, ID_FIELD_NAME, NAME_FIELD_NAME,
                LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME, ID_FIELD_NAME,
                POI_CACHE_NAME, POI_CLASS_NAME)
        ));
    }

    /** */
    private void assertIndexUsed(IgniteCache<?, ?> cache, String sql, String idx)
        throws IgniteCheckedException {
        AtomicReference<String> currPlan = new AtomicReference<>();

        boolean res = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                String plan = explainPlan(cache, sql);

                currPlan.set(plan);

                return plan.contains(idx);
            }
        }, 10_000);

        assertTrue("Query \"" + sql + "\" executed without usage of " + idx + ", see plan:\n\"" +
            currPlan.get() + "\"", res);
    }

    /** */
    private String explainPlan(IgniteCache<?, ?> cache, String sql) {
        return cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema(POI_SCHEMA_NAME))
            .getAll().get(0).get(0).toString().toLowerCase();
    }
}

