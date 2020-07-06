package org.apache.ignite.internal.processors.cache.my_sandbox;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * В классе рассматривается возможность одновременного использования SQL и методов put/get. Не получается одновременно
 * их использовать - происходят разные ошибки.
 * <p>
 * Замечания: 1. Если указать KEY_TYPE=Integer вместо KEY_TYPE=java.lang.Integer то падает сервер
 * https://issues.apache.org/jira/browse/IGNITE-7113 говорится: type "Integer" (without java.lang) is treated as custom
 * type and is not recommended to use.
 */
public class ThinClientInsertAndPut extends GridCommonAbstractTest {
    static final String tableName = "SimpleTable1";
    static final String cacheName = "SimpleTable1";

    @Test
    public void insertAdnPut() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)).setAddresses("localhost:10800"))
        ) {
            client.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + tableName +
                " (id int PRIMARY KEY, username varchar, fakefield boolean) " +
                "WITH " +
                "\"atomicity=transactional," +
                "template=partitioned," +
                "CACHE_NAME=" + cacheName +
//                ", KEY_TYPE=Integer," +
                " VALUE_TYPE=" + MyValue.class.getName() + "," +
                "\"")).getAll();

            client.query(new SqlFieldsQuery("insert into " + tableName + " (id, username, fakefield) values(3, 'asddas', true)")).getAll();

            final ClientCache<Integer, MyValue> simpleTable = client.getOrCreateCache(cacheName);
            simpleTable.put(4, new MyValue(4, "put_value", true));

            assertNotNull(simpleTable.get(3));
            assertNotNull(simpleTable.get(4));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try (final IgniteClient client = Ignition.startClient(new ClientConfiguration().setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)).setAddresses("localhost:10800"))) {
            client.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + tableName +
                " (id int PRIMARY KEY, username varchar, fakefield boolean) " +
                "WITH " +
                "\"atomicity=transactional," +
                "template=partitioned," +
                "CACHE_NAME=" + cacheName +
                ", KEY_TYPE=Integer," +
                " VALUE_TYPE=" + MyValue.class.getName() + "," +
                "\"")).getAll();

            client.query(new SqlFieldsQuery("insert into " + tableName + " (id, username, fakefield) values(3, 'asddas', true)")).getAll();

            final ClientCache<Integer, MyValue> simpleTable = client.getOrCreateCache(cacheName);
            simpleTable.put(4, new MyValue(4, "put_value", true));

            System.out.println(simpleTable.get(3));
            System.out.println(simpleTable.get(4));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class MyValue {
        private Integer id;
        private String username;
        private boolean fakefield;

        public MyValue(Integer id, String username, boolean fakefield) {
            this.id = id;
            this.username = username;
            this.fakefield = fakefield;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public boolean isFakefield() {
            return fakefield;
        }

        public void setFakefield(boolean fakefield) {
            this.fakefield = fakefield;
        }
    }

}
