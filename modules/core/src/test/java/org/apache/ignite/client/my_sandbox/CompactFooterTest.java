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
 * TODO пока не удалось получить ошибку
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

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return new IgniteConfiguration()
            .setIgniteInstanceName(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList("localhost:47500"))))
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));
    }

    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Test
    public void cannotFindSchemaWithCompactFooter() throws Exception {
        final String cacheName = "cache_compact_footer";

        final IgniteEx server = startGrid("serverTest");
        server.cluster().state(ClusterState.ACTIVE);
        server.getOrCreateCache(cacheName).put("my_name", "value_1");
//        final IgniteEx client = startClientGrid("clientTest");

        final ClientConfiguration thinCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        try (final IgniteClient clientThin = Ignition.startClient(thinCfg)) {
            final ClientCache<String, String> cacheThin = clientThin.getOrCreateCache(cacheName);

            final String valueThin = cacheThin.get("my_name");

            assertNotNull(valueThin);

            final List<Cache.Entry<String, String>> entries = cacheThin.query(new ScanQuery<String, String>((k, v) -> "value_1".equals(v))).getAll();

            assertNotNull(entries);
            assertFalse(entries.isEmpty());
            assertEquals("value_1", entries.iterator().next().getValue());
        }
    }

    public static void main(String[] args) {
        final String key = "Key3";
        final String value = "Key3_value";

        final IgniteConfiguration serverCfg = new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList("localhost:47500"))));

        final ClientConfiguration clientCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        try (Ignite server = Ignition.start(serverCfg);
             IgniteClient client = Ignition.startClient(clientCfg)) {

            server.cluster().active(true);

            final ClientCache<String, String> test1 = client.getOrCreateCache(new ClientCacheConfiguration().setName("test_compact_footer"));

            test1.put(key, value);

            final List<Cache.Entry<String, String>> all = test1.query(new ScanQuery<String, String>((s, s2) -> s.startsWith(key))).getAll();
            all.forEach(stringStringEntry -> System.out.println(stringStringEntry.getKey() + ": " + stringStringEntry.getValue()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
