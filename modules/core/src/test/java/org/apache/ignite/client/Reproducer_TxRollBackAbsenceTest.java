package org.apache.ignite.client;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.log4j.Level.DEBUG;
import static org.junit.Assert.assertNotEquals;

/**
 * Test from https://github.com/shishkovilja/ignite/blob/37c8eaf07c80b4dcf37df6e70e2844d4bcf33874/modules/core/src/test/java/org/apache/ignite/TxRollBackAbsenceTest.java
 * Test WALs of data nodes for both commit and rollback presense of transactions.
 */
@WithSystemProperty(key = IGNITE_WAL_LOG_TX_RECORDS, value = "true") /* Comment when testing Ignite from GridGain repository */
public class Reproducer_TxRollBackAbsenceTest extends GridCommonAbstractTest {
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(2)
            .cluster()
            .active(true);
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        resetLog4j(DEBUG, true, "org.apache.ignite");

        // Uncomment this when testing Ignite from GridGain repository
//        withSystemProperty(IGNITE_WAL_LOG_TX_RECORDS, "true");
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setPageSize(DFLT_PAGE_SIZE)
            .setWalMode(FSYNC);

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * Check that WAL records of commits and rollbacks records contained in WALs for one cache transaction on one data
     * node.
     */
    @Test
    public void testOneCacheOneNode() throws Exception {
        String clientName = "CLIENT0";
        String cacheName = "CACHE";

        IgniteEx client = startGrid(getClientCfg(clientName));

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(getCacheCfg(cacheName));

        awaitPartitionMapExchange();

        List<Integer> keys = primaryKeys(grid(0).cache(cacheName), 2);
        Integer k0 = keys.get(0);
        Integer k1 = keys.get(1);

        doInTransaction(client, PESSIMISTIC, READ_COMMITTED, () -> {
            cache.put(k0, 0);
            cache.put(k1, 1);

            return null;
        });

        assertEquals(0, cache.get(k0).intValue());
        assertEquals(1, cache.get(k1).intValue());

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(k0, 3);
            cache.put(k1, 4);

            tx.rollback();
        }

        assertNotEquals(3, cache.get(k0).intValue());
        assertNotEquals(4, cache.get(k1).intValue());

        checkTxStatesInGridsWals(1, 1, grid(0));

        cache.destroy();

        awaitPartitionMapExchange();
    }

    /**
     * Check that WAL records of commits and rollbacks records contained in WALs for one cache transaction for two data
     * nodes.
     */
    @Test
    public void testOneCacheTwoNodes() throws Exception {
        String clientName = "CLIENT1";
        String cacheName = "CACHE";

        IgniteEx client = startGrid(getClientCfg(clientName));

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(getCacheCfg(cacheName));

        awaitPartitionMapExchange();

        Integer k0 = primaryKey(grid(0).cache(cacheName));
        Integer k1 = primaryKey(grid(1).cache(cacheName));

        doInTransaction(client, PESSIMISTIC, READ_COMMITTED, () -> {
            cache.put(k0, 0);
            cache.put(k1, 1);

            return null;
        });

        assertEquals(0, cache.get(k0).intValue());
        assertEquals(1, cache.get(k1).intValue());

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(k0, 3);
            cache.put(k1, 4);

            tx.rollback();
        }

        assertNotEquals(3, cache.get(k0).intValue());
        assertNotEquals(4, cache.get(k1).intValue());

        checkTxStatesInGridsWals(1, 1, grid(0), grid(1));

        cache.destroy();

        awaitPartitionMapExchange();
    }

    /**
     * Check that WAL records of commits and rollbacks records contained in WALs for cross-cache transaction on two data
     * nodes.
     */
    @Test
    public void testTwoCachesTwoNodes() throws Exception {
        String clientName = "CLIENT2";
        String cacheName0 = "CACHE_0";
        String cacheName1 = "CACHE_1";

        IgniteEx client = startGrid(getClientCfg(clientName));

        IgniteCache<Integer, Integer> cache0 = client.getOrCreateCache(getCacheCfg(cacheName0));
        IgniteCache<Integer, Integer> cache1 = client.getOrCreateCache(getCacheCfg(cacheName1));

        awaitPartitionMapExchange();

        Integer k0 = primaryKey(grid(0).cache(cacheName0));
        Integer k1 = primaryKey(grid(1).cache(cacheName1));

        doInTransaction(client, PESSIMISTIC, READ_COMMITTED, () -> {
            cache0.put(k0, 0);
            cache1.put(k1, 1);

            return null;
        });

        assertEquals(0, cache0.get(k0).intValue());
        assertEquals(1, cache1.get(k1).intValue());

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache0.put(k0, 3);
            cache1.put(k1, 4);

            tx.rollback();
        }

        assertNotEquals(3, cache0.get(k0).intValue());
        assertNotEquals(4, cache1.get(k1).intValue());

        checkTxStatesInGridsWals(1, 1, grid(0), grid(1));

        cache0.destroy();
        cache1.destroy();

        awaitPartitionMapExchange();
    }

    /**
     * Prepare client node configuration.
     *
     * @param clientName Client name.
     */
    private IgniteConfiguration getClientCfg(String clientName) throws Exception {
        return super.getConfiguration(clientName)
            .setClientMode(true);
    }

    /**
     * Prepare cache configuration.
     *
     * @param cacheName Cache name.
     */
    private <K, V> CacheConfiguration<K, V> getCacheCfg(String cacheName) {
        return new CacheConfiguration<K, V>()
            .setName(cacheName)
            .setAtomicityMode(TRANSACTIONAL);
    }

    /**
     * Check that expected counts of transactions with {@link TransactionState#COMMITTED} and {@link
     * TransactionState#ROLLED_BACK} if sound WALs of given grids.
     *
     * @param expectedCommitCnt   Expected commits count.
     * @param expectedRollbackCnt Expected rollbacks count.
     * @param grids               Ingnite instances, whick WALs will be checked.
     */
    private void checkTxStatesInGridsWals(int expectedCommitCnt, int expectedRollbackCnt, IgniteEx... grids)
        throws IgniteCheckedException {
        for (IgniteEx grid : grids) {
            List<TxRecord> txRecords = getTxRecordsFromWal(grid);

            assertEquals(expectedCommitCnt, txRecords.stream().filter(tx -> tx.state() == COMMITTED).count());
            assertEquals(expectedRollbackCnt, txRecords.stream().filter(tx -> tx.state() == ROLLED_BACK).count());
        }
    }

    /**
     * Get all records of type {@link TxRecord} from WAL for the given Ignite
     *
     * @param ignite Ignite instance, which WAL will be used to find for TX records.
     */
    private List<TxRecord> getTxRecordsFromWal(IgniteEx ignite) throws IgniteCheckedException {
        List<TxRecord> txRecordList = new ArrayList<>();

        IgniteConfiguration cfg = ignite.configuration();
        Path walPath = Paths.get(cfg.getWorkDirectory(),
            cfg.getDataStorageConfiguration().getWalPath());

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IgniteWalIteratorFactory.IteratorParametersBuilder builder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder().pageSize(DFLT_PAGE_SIZE);

        builder.filesOrDirs(walPath.toFile());

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> t = it.nextX();
                WALRecord record = t.get2();

                if (record.type() == TX_RECORD) {
                    log.warning("WALRecord found: " + record.toString());
                    txRecordList.add((TxRecord)record);
                }
            }
        }

        return txRecordList;
    }
}
