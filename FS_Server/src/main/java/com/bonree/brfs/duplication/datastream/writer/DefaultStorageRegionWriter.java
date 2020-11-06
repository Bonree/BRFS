package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.client.utils.Strings;
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
    public void write(String srName, DataItem[] items, StorageRegionWriteCallback callback) {
        DataEngine dataEngine = dataEngineManager.getDataEngine(srName);
        if (dataEngine == null) {
            LOG.error("can not get data engine by region[{}]", srName);
            callback.error(new RuntimeException(
                Strings.format("No data engine is found for storageRegion[%s]", srName)));
            return;
        }

        AtomicReferenceArray<String> fids = new AtomicReferenceArray<>(items.length);
        AtomicInteger count = new AtomicInteger(items.length);
        for (int i = 0; i < items.length; i++) {
            dataEngine.store(items[i].getBytes(), new DataCallback(i, fids, count, callback));
        }
    }

    public void write(String srName, byte[] data, StorageRegionWriteCallback callback) {
        try {
            DataEngine dataEngine = dataEngineManager.getDataEngine(srName);
            if (dataEngine == null) {
                LOG.error("can not get data engine by region[{}]", srName);
                callback.error(new RuntimeException(
                    Strings.format("No data engine is found for storageRegion[%s]", srName)));
                return;
            }
            dataEngine.store(data, new SingleDataCallback(callback));
        } catch (Exception e) {
            LOG.error("error when srWriter write the data");
            callback.error(e);
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

        @Override
        public void error(Exception e) {
            callback.error(e.getCause());
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

        @Override
        public void error(Exception e) {
            callback.error(e.getCause());
        }

    }
}
