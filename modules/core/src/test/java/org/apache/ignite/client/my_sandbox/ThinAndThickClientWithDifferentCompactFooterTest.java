package org.apache.ignite.client.my_sandbox;

import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.FunctionalTest;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Тесты связанные с различными настройками CompactFooter
 * <p>
 * Дополнительно можно посмотреть {@link FunctionalTest}
 */
public class ThinAndThickClientWithDifferentCompactFooterTest extends GridCommonAbstractTest {
    /**
     * Если положить значение при помощи dataStreamer или толстого клиента по ключу {@link KeyObj}, то тонкией клиент не
     * может получить значение по этому ключу.
     */
    @Test
    public void makeTest() throws Exception {
        final String cacheName = "MyCache";
        final KeyObj keyObj = new KeyObj("my_name");

        startGrid("serverTest");
        IgniteEx clientThick = startClientGrid("clientTest");

        IgniteCache<KeyObj, String> cacheThick = clientThick.getOrCreateCache(cacheName);

        /*try (final IgniteDataStreamer<KeyObj, String> streamer = clientThick.dataStreamer(cacheName)) {
            streamer.addData(keyObj, "Value_1");
        }*/
        cacheThick.put(keyObj, "value_1");

        String valueThick = cacheThick.get(keyObj);

        assertNotNull(valueThick);

        try (final IgniteClient clientThin = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            final ClientCache<KeyObj, String> cacheThin = clientThin.getOrCreateCache(cacheName);

            final String valueThin = cacheThin.get(keyObj);

            assertNotNull(valueThin);
        }
    }

    public static class KeyObj {
        private String name;

        public KeyObj(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            KeyObj obj = (KeyObj)o;
            return name.equals(obj.name);
        }

        @Override public int hashCode() {
            return Objects.hash(name);
        }
    }
}
