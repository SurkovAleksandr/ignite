package org.apache.ignite.internal.processors.cache.mytests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateWithSubQueryTest extends GridCommonAbstractTest {
    @Before
    public void before() {

    }

    /**
     * Creat two tables: 'peson' and 'counter'.
     * Populate them. Using a SQL subquery to update the 'counter' table.
     * Field 'counter.count' is expected to be equal to the number of rows in the 'person' table .
     */
    @Test
    public void updateWithSubQueryTest() throws Exception {
        final String CACHE_PERSON = "person";
        final String CACHE_COUNTER = "counter";

        final Ignite ignite = startGridsMultiThreaded(2);

        final IgniteEx client = startClientGrid();

        final IgniteCache<Integer, Person> cachePerson = client.createCache(new CacheConfiguration<Integer, Person>(CACHE_PERSON)
                //.setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Person.class).setTableName(CACHE_PERSON)))
                .setIndexedTypes(Integer.class, Person.class)
                // If create REPLICATED cache, request will be success.
                //.setCacheMode(CacheMode.REPLICATED)
                .setSqlSchema("PUBLIC"));

        final IgniteCache<Integer, Counter> cacheCounter = client.createCache(new CacheConfiguration<Integer, Counter>(CACHE_COUNTER)
                .setQueryEntities(Collections.singletonList(
                        new QueryEntity(Integer.class, Counter.class)
                                .setTableName(CACHE_COUNTER)
                ))
                .setSqlSchema("PUBLIC"));

        // Prepare two keys for different nodes
        ClusterNode node0 = ignite(0).cluster().localNode();
        ClusterNode node1 = ignite(1).cluster().localNode();

        Integer id0 = keyForNode(client.affinity(CACHE_PERSON), new AtomicInteger(1), node0);
        Integer id1 = keyForNode(client.affinity(CACHE_PERSON), new AtomicInteger(1), node1);

        // Populate table with two value on different node
        sql(cachePerson, "insert into " + CACHE_PERSON + "(_key, id, name) values(?,?,?), (?,?,?)",
                id0, id0, "person0",
                id1, id1, "person0");

        // Check that two values are inserted
        assertEquals(2, cachePerson.query(new SqlFieldsQuery("select * from " + CACHE_PERSON)).getAll().size());

        // Create row in other table for future update
        sql(cacheCounter, "insert into " + CACHE_COUNTER + "(_key, count) values(1,0)");

        // Check that 'counter.count' is correct
        assertEquals(0, cacheCounter.get(1).getCount().intValue());

        // Update 'counter.count' using SQL subquery
        sql(cacheCounter,"update " + CACHE_COUNTER + " set count=(select count(*) from " + CACHE_PERSON +") " +
                        "where _key=1");

        // Check that there are two rows in table 'person'
        List<List<?>> countResult = sql(cachePerson, "select count(*) from " + CACHE_PERSON);
        assertEquals(Long.valueOf(2), ((Long)countResult.get(0).get(0)));

        // Check that 'counter.count' are updated correct
        assertEquals(2, cacheCounter.get(1).getCount().intValue());

    }

    private List<List<?>> sql(IgniteCache<?,?> cache, String sql, Object ... args) {
        final SqlFieldsQuery query = new SqlFieldsQuery(sql);
        if (args.length > 0)
            query.setArgs(args);

        return cache.query(query).getAll();
    }

    public static class Person {
        @QuerySqlField()
        private Integer id;
        @QuerySqlField
        @AffinityKeyMapped
        private String name;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Counter {
        @QuerySqlField
        private Integer count;

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }
}
