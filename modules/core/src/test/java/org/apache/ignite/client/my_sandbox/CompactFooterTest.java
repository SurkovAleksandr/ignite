package org.apache.ignite.client.my_sandbox;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.marshaller.optimized.TestTcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Тест падает на 2.7.6. На 2.8 не падает.
 * Пример ошибки:
 * org.apache.ignite.internal.client.thin.ClientServerError: Ignite failed to process request [102]:
 *      Cannot find schema for object with compact footer [typeName=com.ignite_se.clients_examples.thin_client.scan_query.ScanQueryByTextPredicate, typeId=-614392894, missingSchemaId=1955455756, existingSchemaIds=[]] (server status code [1])
 *                 at org.apache.ignite.internal.client.thin.TcpClientChannel.processNextResponse(TcpClientChannel.java:333)
 *                 at org.apache.ignite.internal.client.thin.TcpClientChannel.receive(TcpClientChannel.java:241)
 *                 at org.apache.ignite.internal.client.thin.TcpClientChannel.service(TcpClientChannel.java:178)
 *                 at org.apache.ignite.internal.client.thin.ReliableChannel.service(ReliableChannel.java:160)
 *                 at org.apache.ignite.internal.client.thin.GenericQueryPager.next(GenericQueryPager.java:71)
 *                 at org.apache.ignite.internal.client.thin.ClientQueryCursor$1.nextPage(ClientQueryCursor.java:93)
 *                 at org.apache.ignite.internal.client.thin.ClientQueryCursor$1.hasNext(ClientQueryCursor.java:76)
 *                 at org.apache.ignite.internal.client.thin.ClientQueryCursor.getAll(ClientQueryCursor.java:47)
 *                 at com.ignite_se.clients_examples.thin_client.scan_query.ThinClientScanQuery.main(ThinClientScanQuery.java:36)
 * */
public class CompactFooterTest extends GridCommonAbstractTest {

    @Test
    public void testCannotFindSchemaWithCompactFooterTest() throws Exception {
        final String cacheName = "cache_compact_footer";
        final String key = "key";
        final String value = "key_value";

        final IgniteEx server = startGrid("serverTest");
        server.cluster().active(true);
        server.getOrCreateCache(cacheName).put(key, value);

        final ClientConfiguration thinCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        try (final IgniteClient clientThin = Ignition.startClient(thinCfg)) {

            final ClientCache<String, String> cacheThin = clientThin.getOrCreateCache(cacheName);

            final String valueThin = cacheThin.get(key);

            assertNotNull(valueThin);

            final List<Cache.Entry<String, String>> entries = cacheThin.query(new ScanQuery<String, String>((k, v) -> value.equals(v))).getAll();

            assertNotNull(entries);
            assertFalse(entries.isEmpty());
            assertEquals(value, entries.iterator().next().getValue());
        }
    }
 }
