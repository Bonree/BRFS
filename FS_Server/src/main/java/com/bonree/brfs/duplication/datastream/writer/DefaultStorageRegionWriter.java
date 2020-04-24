package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.datastream.dataengine.DataStoreCallback;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.inject.Inject;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorageRegionWriter implements StorageRegionWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageRegionWriter.class);

    private DataEngineManager dataEngineManager;

    @Inject
    public DefaultStorageRegionWriter(DataEngineManager dataEngineManager) {
        this.dataEngineManager = dataEngineManager;
    }

    @Override
    public void write(int storageRegionId, DataItem[] items, StorageRegionWriteCallback callback) {
        DataEngine dataEngine = dataEngineManager.getDataEngine(storageRegionId);
        if (dataEngine == null) {
            LOG.error("can not get data engine by region[id={}]", storageRegionId);
            callback.error();
            return;
        }

        AtomicReferenceArray<String> fids = new AtomicReferenceArray<>(items.length);
        AtomicInteger count = new AtomicInteger(items.length);
        for (int i = 0; i < items.length; i++) {
            if (items[i].getBytes() == null) {
                LOG.error("write erro because of null bytes");
                callback.error();
                continue;
            }
            dataEngine.store(items[i].getBytes(), new DataCallback(i, fids, count, callback));
        }
    }

    public void write(int storageRegionId, byte[] data, StorageRegionWriteCallback callback) {
        try {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            DataEngine dataEngine = dataEngineManager.getDataEngine(storageRegionId);
            stopWatch.split();
            LOG.info("require a dataEngine cost [{}]", stopWatch.getSplitTime());
            if (dataEngine == null) {
                LOG.error("can not get data engine by region[id={}]", storageRegionId);
                callback.error();
                return;
            }
            if (data == null) {
                LOG.error("null data to write into the datapool！");
                callback.error();
                return;
            }
            dataEngine.store(data, new SingleDataCallback(callback));
            stopWatch.split();
            LOG.info("enqueue the datapool cost [{}]", stopWatch.getSplitTime());
            stopWatch.stop();
        } catch (Exception e) {
            LOG.error("error when srWriter write the data");
            callback.error();
        }
    }

    private static class SingleDataCallback implements DataStoreCallback {
        private StorageRegionWriteCallback callback;

        public SingleDataCallback(StorageRegionWriteCallback callback) {
            this.callback = callback;
        }

        @Override
        public void dataStored(String storeToken) {
            callback.complete(storeToken);
        }

    }

    private static class DataCallback implements DataStoreCallback {
        private final int index;
        private AtomicReferenceArray<String> fids;
        private AtomicInteger count;
        private StorageRegionWriteCallback callback;

        public DataCallback(int index, AtomicReferenceArray<String> fids, AtomicInteger count,
                            StorageRegionWriteCallback callback) {
            this.index = index;
            this.fids = fids;
            this.count = count;
            this.callback = callback;
        }

        @Override
        public void dataStored(String storeToken) {
            fids.set(index, storeToken);

            if (count.decrementAndGet() == 0) {
                String[] results = new String[fids.length()];
                for (int i = 0; i < results.length; i++) {
                    results[i] = fids.get(i);
                }

                callback.complete(results);
            }
        }

    }
}
