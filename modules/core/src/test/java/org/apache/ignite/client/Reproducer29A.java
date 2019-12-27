package org.apache.ignite.client;

import java.lang.management.ManagementFactory;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class Reproducer29A extends GridCommonAbstractTest {
    /**
     * Wait condition timeout.
     */
    private static final long WAIT_CONDITION_TIMEOUT = 10_000L;

    /**
     * Test value for cache.
     */
    private static final CardInfo TEST_VALUE = new CardInfo();

    /**
     * Ignite to string include sensitive.
     */
    private static boolean igniteToStringIncludeSensitive;

    private static final LogListener LSNR = LogListener
        .matches(TEST_VALUE.toString())
        .build();

    /**
     *
     */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        log.registerListener(LSNR);
        cfg.setGridLogger(log);

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE,
            Boolean.toString(igniteToStringIncludeSensitive));

        return cfg;
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void enabledIgniteToStringIncludeSensitiveTest() throws Exception {
        igniteToStringIncludeSensitive = true;

        igniteToStringIncludeSensitiveTest();

        assertTrue(INCLUDE_SENSITIVE);

        assertTrue(LSNR.check());
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void disabledIgniteToStringIncludeSensitiveTest() throws Exception {
        igniteToStringIncludeSensitive = false;

        igniteToStringIncludeSensitiveTest();

        assertFalse(INCLUDE_SENSITIVE);

        assertFalse(LSNR.check());
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        LSNR.reset();
    }

    /**
     * @throws Exception If fails.
     */
    public void igniteToStringIncludeSensitiveTest() throws Exception {
        IgniteEx ig = startGrids(2);

        checkSetTxTimeoutDuringPartitionMapExchange(ig);
    }

    /**
     * @param ig Ignite instance where deadlock tx will start.
     * @throws Exception If fails.
     */
    private void checkSetTxTimeoutDuringPartitionMapExchange(IgniteEx ig) throws Exception {
        final long longTimeout = 600_000L;
        final long shortTimeout = 5_000L;

        TransactionsMXBean mxBean = txMXBean(0);

        // Set very long txTimeoutOnPME, transaction should be rolled back.
        mxBean.setTxTimeoutOnPartitionMapExchange(longTimeout);
        assertTxTimeoutOnPartitionMapExchange(longTimeout);

        AtomicReference<Exception> txEx = new AtomicReference<>();

        IgniteInternalFuture<Long> fut = startDeadlock(ig, txEx, 0);

        startGridAsync(2);

        waitForExchangeStarted(ig);

        mxBean.setTxTimeoutOnPartitionMapExchange(shortTimeout);

        awaitPartitionMapExchange();

        fut.get();

        assertTrue("Transaction should be rolled back", hasCause(txEx.get(), TransactionRollbackException.class));
    }

    /**
     * Start test deadlock
     *
     * @param ig      Ig.
     * @param txEx    Atomic reference to transaction exception.
     * @param timeout Transaction timeout.
     */
    private IgniteInternalFuture<Long> startDeadlock(Ignite ig, AtomicReference<Exception> txEx, long timeout) {
        IgniteCache<Object, Object> cache = ig.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        AtomicInteger thCnt = new AtomicInteger();

        CyclicBarrier barrier = new CyclicBarrier(2);

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() {
                int thNum = thCnt.incrementAndGet();

                try (Transaction tx = ig.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 0)) {
                    cache.put(thNum, TEST_VALUE.toString());

                    barrier.await();

                    cache.put(thNum % 2 + 1, TEST_VALUE.toString());

                    tx.commit();

                    log();

                }
                catch (Exception e) {
                    txEx.set(e);
                }

                return null;
            }
        }, 2, "tx-thread");
    }

    /**
     * Test data.
     */
    private static class CardInfo {
        private String number;
        private int month;
        private int year;
        private String owner;
        private int pin;
        private int cvv2;

        public CardInfo() {
            number = "5469 3822 1032 2224";
            month = 01;
            year = 22;
            owner = "NAME SURNAME";
            pin = 4444;
            cvv2 = 333;
        }

        @Override public String toString() {
            return new StringJoiner(", ", CardInfo.class.getSimpleName() + "[", "]")
                .add("number='" + number + "'")
                .add("month=" + month)
                .add("year=" + year)
                .add("owner='" + owner + "'")
                .add("pin=" + pin)
                .add("cvv2=" + cvv2)
                .toString();
        }
    }

    /**
     * Starts grid asynchronously and returns just before grid starting. Avoids blocking on PME.
     *
     * @param idx Test grid index.
     * @throws Exception If fails.
     */
    private void startGridAsync(int idx) throws Exception {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(idx);
                }
                catch (Exception e) {
                    // no-op.
                }
            }
        });
    }

    /**
     * Waits for srarting PME on grid.
     *
     * @param ig Ignite grid.
     * @throws IgniteCheckedException If fails.
     */
    private void waitForExchangeStarted(IgniteEx ig) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridDhtPartitionsExchangeFuture fut : ig.context().cache().context().exchange().exchangeFutures()) {
                    if (!fut.isDone())
                        return true;
                }

                return false;
            }
        }, WAIT_CONDITION_TIMEOUT);

        // Additional waiting to ensure that code really start waiting for partition release.
        U.sleep(5_000L);
    }

    /**
     * Checking the transaction timeout on all grids.
     *
     * @param expTimeout Expected timeout.
     * @throws IgniteInterruptedCheckedException If failed.
     */
    private void assertTxTimeoutOnPartitionMapExchange(final long expTimeout)
        throws IgniteInterruptedCheckedException {

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite ignite : G.allGrids()) {
                    long actualTimeout = ignite.configuration()
                        .getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

                    if (actualTimeout != expTimeout) {
                        log.warning(String.format(
                            "Wrong transaction timeout on partition map exchange [grid=%s, timeout=%d, expected=%d]",
                            ignite.name(), actualTimeout, expTimeout));

                        return false;
                    }
                }

                return true;

            }
        }, WAIT_CONDITION_TIMEOUT));
    }

    /**
     *
     */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }

}

