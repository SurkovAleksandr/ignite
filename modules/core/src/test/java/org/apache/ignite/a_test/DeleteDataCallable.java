package org.apache.ignite.a_test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.a_test.Reproducer_DeleteDataFromCacheTest.CACHE_NAME_1;

public class DeleteDataCallable implements IgniteCallable<String> {
    @IgniteInstanceResource
    private IgniteEx ignite;
    @LoggerResource
    private IgniteLogger log;

    @Override public String call() throws Exception {
        final int[] primaryPartitions = ignite.affinity(CACHE_NAME_1).primaryPartitions(ignite.localNode());

        final IgniteInternalCache<Object, Object> cache = ignite.cachex(CACHE_NAME_1).keepBinary();

        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Long>> futureList = new ArrayList<>();
        for (int partitionId : primaryPartitions) {
            //futureList.add(executorService.submit(new DeleteDataOnIterator(i, cache)));
            futureList.add(executorService.submit(new DeleteDataAfterIteration3(partitionId, cache, log)));
        }

        futureList.forEach(future -> {
            try {
                future.get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        return null;
    }

    private static class DeleteDataAfterIteration3 implements Callable<Long> {
        private int partitionId;
        private IgniteInternalCache<Object, Object> cache;
        private static final int countInBatch = 100;
        private IgniteLogger log;

        public DeleteDataAfterIteration3(int partitionId, IgniteInternalCache<Object, Object> cache, IgniteLogger log) {
            this.partitionId = partitionId;
            this.cache = cache;
            this.log = log;
        }

        @Override public Long call() throws Exception {
            final GridCacheContext<Object, Object> context = cache.context();
            final GridIterator<CacheDataRow> partitionIterator = context.offheap().cachePartitionIterator(context.cacheId(), partitionId);

            long countDeleted = 0;

            List<KeyCacheObject> keyCacheObjectList = defindObjectForDelete(partitionIterator);
            while (!keyCacheObjectList.isEmpty()) {
                cache.removeAll(keyCacheObjectList);
                countDeleted += keyCacheObjectList.size();
                keyCacheObjectList.clear();
                keyCacheObjectList = defindObjectForDelete(partitionIterator);
            }

            return countDeleted;
        }

        private List<KeyCacheObject> defindObjectForDelete(GridIterator<CacheDataRow> partitionIterator) {
            List<KeyCacheObject> keyCacheObjectList = new ArrayList<>(1000);
            while (partitionIterator.hasNext()) {
                final CacheDataRow row = partitionIterator.next();
                final BinaryObject value = (BinaryObject)row.value();

                if ((int)value.field("age") > 10) {
                    keyCacheObjectList.add(row.key());
                }

                /*if(keyCacheObjectList.size() == 100) {
                    break;
                }*/
            }

            return keyCacheObjectList;
        }
    }

    // todo итератор не поддерживается(UnsupportedOperationException)
    private static class DeleteDataOnIterator implements Callable<Long> {
        @LoggerResource
        IgniteLogger log;

        private int partitionId;
        private IgniteInternalCache<Object, Object> cache;

        public DeleteDataOnIterator(int partitionId, IgniteInternalCache<Object, Object> cache) {
            this.partitionId = partitionId;
            this.cache = cache;
        }

        @Override public Long call() throws Exception {
            final GridCacheContext<Object, Object> context = cache.context();
//            todo не понятно как использовать итератор
//            final Iterator<Cache.Entry<Object, Object>> partitionIterator = cache.iterator();
//            final GridIterator<CacheDataRow> partitionIterator = context.offheap().cachePartitionIterator(context.cacheId(), partitionId);
            final GridIterator<CacheDataRow> partitionIterator = context.offheap().partitionIterator(partitionId);

            long countDeleted = 0;
            while (partitionIterator.hasNext()) {
                final BinaryObject value = (BinaryObject)partitionIterator.next();

                if ((int)value.field("age") > 10) {
                    partitionIterator.remove();
                    countDeleted++;
                }
            }
            return countDeleted;
        }
    }
}
