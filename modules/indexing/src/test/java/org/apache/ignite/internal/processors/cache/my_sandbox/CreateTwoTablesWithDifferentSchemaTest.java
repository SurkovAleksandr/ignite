package org.apache.ignite.internal.processors.cache.my_sandbox;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * 1. Интересные примеры с транзакциями org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeTest 2. Создание
 * Ignite через прокси org.apache.ignite.Ignition#ignite(java.lang.String)
 */
public class CreateTwoTablesWithDifferentSchemaTest extends GridCommonAbstractTest {
    /**
     * Create two tables/caches with the same VALUE_TYPE
     */
    @Test
    public void executeTest() throws Exception {
        try (final Ignite server1 = Ignition.start(Config.getServerConfiguration());
             final Ignite server2 = Ignition.start(Config.getServerConfiguration());
             final IgniteClient client = Ignition.startClient(new ClientConfiguration()
                 .setAddresses(Config.SERVER)
                 .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)))) {

            createTableAndInsert(client, "MyTable1", "int");
            createTableAndInsert(client, "MyTable2", "varchar");
        }
    }

    private void createTableAndInsert(IgniteClient client, String tableName, String typeField) {
        /**
         * Can use tableName instead of MyObj.class.getName(), but then you can’t use the {@link ClientCache#get(Object)} method.
         * */
        client.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + tableName + " (id int primary key, value " + typeField + ") " +
            "WITH \"VALUE_TYPE=" + MyObj.class.getName() + ",CACHE_NAME=" + tableName + ",atomicity=transactional,template=partitioned,backups=1\"")
        ).getAll();

        final Object value = "int".equals(typeField) ? 1 : "value_1";

        try {
            client.query(new SqlFieldsQuery("INSERT INTO " + tableName + " (id, value) values(?,?)")
                .setArgs(1, value)
            ).getAll();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    static class MyObj {
        private Integer id;
        //It is assumed that the type of this property will depend on the type of field in the table.
        private Object value;

        public MyObj(Integer id, Object value) {
            this.id = id;
            this.value = value;
        }
    }

    @Test
    public void storeBinaryInTableTest() throws Exception {
        final String tableName = "MyTable";
        final String valueType = "MiBinaryObj";

        Ignite ignite = startGrid(0);
        try (final IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)))) {

            client.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + tableName + " (id int primary key, value " + "varchar" + ") " +
                "WITH \"VALUE_TYPE=" + valueType + ",CACHE_NAME=" + tableName + ",atomicity=transactional,template=partitioned,backups=1\"")
            ).getAll();

            client.query(new SqlFieldsQuery("INSERT INTO " + tableName + " (id, value) values(?,?)")
                .setArgs(1, "value_field_1")
            ).getAll();

            BinaryObjectBuilder builder = ignite.binary().builder(valueType);

            builder.setField("id", 2);
            builder.setField("value", "value_field_2");

            BinaryObject binObj = builder.build();

            ClientCache<Integer, BinaryObject> cache = client.cache(tableName).withKeepBinary();

            cache.put(2, binObj);

            final List<List<?>> all = cache.query(new SqlFieldsQuery("SELECT * FROM " + tableName)).getAll();

            assertEquals(2, all.size());
            assertNotNull(all.get(0).get(0));
            assertNotNull(all.get(1).get(0));

            final BinaryObject obj1 = cache.get(1);
            final BinaryObject obj2 = cache.get(1);

            assertNotNull(obj1);
            //id field is absent in the object obj1, but exists in obj2
            assertNotNull(obj1.field("id"));
            assertNotNull(obj2.field("id"));

            //A binary object has two fields in different letter case
            assertNotNull(obj1.field("value"));
            assertNotNull(obj1.field("VALUE"));
        }
    }
}
