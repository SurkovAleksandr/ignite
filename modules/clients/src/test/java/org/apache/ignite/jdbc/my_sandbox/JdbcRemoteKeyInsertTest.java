package org.apache.ignite.jdbc.my_sandbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.Config;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertTimeout;

/**
 * Example taken from https://cwiki.apache.org/confluence/display/IGNITE/IEP-41%3A+Security+Context+of+thin+client+on+remote+nodes
 * */
public class JdbcRemoteKeyInsertTest extends AbstractSecurityTest {
    /** */
    public static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** */
    private static final String TEST_CACHE = "test-cache";

    /** */
    private static final String TEST_SCHEMA = "test_schema";

    /** */
    private static final String JDBC_CLIENT = "jdbc-client";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(
            getConfiguration(0,
                new TestSecurityData(
                    JDBC_CLIENT,
                    cachePermissions(TEST_CACHE, CACHE_PUT)
                )
            )
        );

        startGrid(getConfiguration(1)).cluster().state(ACTIVE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(TEST_CACHE);

        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setSqlSchema(TEST_SCHEMA);

        grid(0).createCache(ccfg);
    }

    /** */
    private IgniteConfiguration getConfiguration(int idx, TestSecurityData... users) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(
            instanceName,
            new TestSecurityPluginProvider(
                instanceName,
                null,
                create()
                    .appendSystemPermissions(CACHE_CREATE, JOIN_AS_SERVER)
                    .appendCachePermissions(TEST_CACHE, CACHE_READ)
                    .build(),
                false,
                users
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 1000;
    }

    /** */
    private void doInsert(int nodeIdx) throws Exception {
        int key = keyForNode(nodeIdx);

        String tableName = Integer.class.getSimpleName();

        execute(
            JDBC_CLIENT,
            "INSERT INTO " + TEST_SCHEMA + '.' + tableName + " (_key, _val) VALUES (" + key + ", 0)"
        );

        int val = grid(1).<Integer, Integer>cache(TEST_CACHE).get(key);

        assert val == 0;
    }

    /** */
    @Test
    public void testLocalInsert() throws Exception {
        doInsert(0);
    }

    /** */
    @Test
    public void testRemoteInsert() throws Exception {
        doInsert(1);
    }

    /** */
    private int keyForNode(int nodeIdx) {
        return keyForNode(
            grid(0).affinity(TEST_CACHE),
            new AtomicInteger(0),
            grid(nodeIdx).localNode()
        );
    }

    /** */
    private SecurityPermissionSet cachePermissions(String cacheName, SecurityPermission... perms) {
        return create()
            .defaultAllowAll(false)
            .appendCachePermissions(cacheName, perms)
            .build();
    }

    /** */
    protected void execute(String login, String sql) throws Exception {
        try (Connection conn = getConnection(login)) {
            Statement stmt = conn.createStatement();

            assertTimeout(
                getTestTimeout(),
                MILLISECONDS,
                () -> {
                    try {
                        stmt.execute(sql);
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
    }

    /** */
    protected Connection getConnection(String login) throws SQLException {
        return DriverManager.getConnection(JDBC_URL_PREFIX + Config.SERVER, login, "");
    }
}
