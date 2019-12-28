package org.apache.ignite.internal.processors.cache.authentication;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Reproducer_IGN_129 extends GridCommonAbstractTest {
    private static final String CLIENT_NAME = "client";
    public static final String TABLE_NAME = "A_TABLE";

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setActiveOnStart(false);
        cfg.setClientMode(CLIENT_NAME.equals(name));
        //cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        //cfg.setCacheConfiguration(new CacheConfiguration(TEST_CACHE_NAME));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)));

        return cfg;
    }

    private static class A {
        @QuerySqlField
        private long id;
        @QuerySqlField
        private String name;

        public A(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
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
            A a = (A)o;
            return id == a.id &&
                name.equals(a.name);
        }

        @Override public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    /**
     * Тест воспроизводит ситуацию для задачи IGN-129, когда по каким-то причинам невозможно подгрузить данные из папки
     * binary_meta.
     * <p>
     * Задержка в выполнении метода свзана с: [CacheObjectBinaryProcessorImpl] Schema is missing while no metadata
     * updates are in progress (will wait for schema update within timeout defined by IGNITE_WAIT_SCHEMA_UPDATE system
     * property) [typeId=1771899788, missingSchemaId=970781171, pendingVer=NA, acceptedVer=NA,
     * binMetaUpdateTimeout=30000]
     */
    @Test
    public void errorlLoadinBinaryMeta() throws Exception {
        IgniteEx server = startGrid(1);
        server.cluster().active(true);

        final IgniteEx client = startGrid(CLIENT_NAME);
        IgniteCache<Long, A> cache = createCacheWithTable(client);
        long key = 1L;
        A obj = new A(key, "Vasja" + key);
        cache.put(key, obj);

        assertEquals(obj, cache.get(key));

        final String igniteWorkDir = server.configuration().getWorkDirectory();

        //Название директории, в которой хранится bin файл с meta для класса
        final String folderName = server.context().pdsFolderResolver().resolveFolders().folderName();
        //Этому типу соответствует название файла bit  в директории "binary_meta/" + folderName.
        int typeId = client.binary().type(A.class).typeId();

        stopAllGrids(true);

        //Удаляем файла с мета данными
        Files.delete(Paths.get(igniteWorkDir, StandaloneGridKernalContext.BINARY_META_FOLDER, folderName, typeId + ".bin"));

        server = startGrid(1);
        cache = server.getOrCreateCache(DEFAULT_CACHE_NAME);

        try {
            cache.get(key);
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Cannot find metadata for object with compact footer"));
        }
    }

    public IgniteCache<Long, A> createCacheWithTable(IgniteEx client) {
        CacheConfiguration<Long, A> cacheCfg = new CacheConfiguration<>();
        cacheCfg
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(new QueryEntity(long.class.getName(), A.class.getName())
                .setTableName(TABLE_NAME)
                .setFields(Stream.of(
                    new AbstractMap.SimpleEntry<>("id", long.class.getName()),
                    new AbstractMap.SimpleEntry<>("name", String.class.getName())
                    ).collect(Collectors.toMap(
                    AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new
                    ))
                )
            ));

        IgniteCache<Long, A> cache = client.getOrCreateCache(cacheCfg);

        return cache;
    }
}
