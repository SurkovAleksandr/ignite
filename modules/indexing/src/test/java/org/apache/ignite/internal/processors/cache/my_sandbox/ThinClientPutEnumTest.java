package org.apache.ignite.internal.processors.cache.my_sandbox;

import java.util.Arrays;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ThinClientPutEnumTest extends GridCommonAbstractTest {

    /**
     * Тест падает если добавить индексацию для enum.
     * При этом падает и серверный узел.
     * */
    @Test
    public void putEnumToCacheTest() throws Exception {
        final String cacheName = "cache_enum";
        final Integer key = 1;
        final MyEnum value = MyEnum.TWO;

        final IgniteEx server = startGrid("server");
//        server.cluster().state(ClusterState.ACTIVE);

//        server.getOrCreateCache(cacheName).put(key, value);

        final ClientConfiguration thinCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        try (final IgniteClient clientThin = Ignition.startClient(thinCfg)) {

            final ClientCache<Integer, MyEnum> cacheThin = clientThin.getOrCreateCache(getCacheConfig(cacheName));

            cacheThin.put(key, value);
            final MyEnum valueThin = cacheThin.get(key);

            assertNotNull(valueThin);
        }
    }

    private ClientCacheConfiguration getCacheConfig(String cacheName) {
        //TODO схема не указывается, но почему-то она выставляется по названию кэша, а название кэша выставляется как название класса для значения
        return new ClientCacheConfiguration()
            .setName(cacheName)
            .setQueryEntities(
                new QueryEntity(Integer.class.getName(), MyEnum.class.getName())
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("name", String.class.getName(), null)
                    //TODO Если не индексировать, то тест пройдет
                    .setIndexes(Arrays.asList(
                        new QueryIndex("id")
//                        new QueryIndex().setFields(persFields))
                        )
                    )
//                    .setNotNullFields(new HashSet<>(Arrays.asList("id", "name")))
            );
//            .setQueryEntities()
    }

    enum MyEnum {
        ONE, TWO;
    }

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
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

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
