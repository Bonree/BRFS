package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataStoreCallback;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplier;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataEngine implements DataEngine {
    private static final Logger log = LoggerFactory.getLogger(DefaultDataEngine.class);

    private final DataPool dataPool;
    private final FileObjectSupplier fileSupplier;
    private final DiskWriter diskWriter;

    private final ExecutorService mainThread;

    private final StorageRegionManager storageRegionManager;
    private final String storageRegionName;

    private final AtomicBoolean runningState = new AtomicBoolean(false);
    private volatile boolean quit = false;

    public DefaultDataEngine(String storageRegionName,
                             StorageRegionManager storageRegionManager,
                             DataPool pool,
                             FileObjectSupplier fileSupplier,
                             DiskWriter writer) {
        this.storageRegionName = storageRegionName;
        this.storageRegionManager = storageRegionManager;
        this.dataPool = pool;
        this.fileSupplier = fileSupplier;
        this.diskWriter = writer;
        this.mainThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                                 new LinkedBlockingQueue<>(),
                                                 new PooledThreadFactory("dataengine_" + storageRegionName));

        this.mainThread.execute(new DataProcessor());
    }

    @Override
    public StorageRegion getStorageRegion() {
        return storageRegionManager.findStorageRegionByName(storageRegionName);
    }

    @Override
    public void store(byte[] data, DataStoreCallback callback) {
        try {
            if (data == null) {
                callback.dataStored(null);
                return;
            }

            dataPool.put(new DataObject() {

                @Override
                public int length() {
                    return data.length;
                }

                @Override
                public byte[] getBytes() {
                    return data;
                }

                @Override
                public void processComplete(String result) {
                    callback.dataStored(result);
                }
            });
        } catch (InterruptedException e) {
            log.error("store data failed", e);
            callback.dataStored(null);
        }
    }

    @Override
    public void close() throws IOException {
        quit = true;
        mainThread.shutdownNow();
    }

    private class DataProcessor implements Runnable {

        @Override
        public void run() {
            if (!runningState.compareAndSet(false, true)) {
                log.error("can not execute data engine again, because it's started!",
                          new IllegalStateException("Data engine has been started!"));
                return;
            }

            DataObject unhandledData = null;

            while (!quit || !dataPool.isEmpty()) {
                try {
                    DataObject data = unhandledData == null ? (unhandledData = dataPool.take()) : unhandledData;

                    log.debug("fetch file with {}", data.length());
                    FileObject file = fileSupplier.fetch(data.length());
                    unhandledData = null;

                    if (file == null) {
                        data.processComplete(null);
                        continue;
                    }

                    List<DataObject> dataList = new ArrayList<>();
                    dataList.add(data);

                    while (true) {
                        data = dataPool.peek();
                        if (data == null) {
                            break;
                        }

                        if (!file.apply(data.length())) {
                            break;
                        }

                        dataList.add(data);
                        dataPool.remove();
                    }

                    log.debug("out => {}", file.node().getName());
                    diskWriter.write(file, dataList, (file1, errorOccurred, isClosed) -> {
                        log.debug("in => {}, sync => {}", file1.node().getName(), errorOccurred);
                        if (!isClosed) {
                            fileSupplier.recycle(file1, errorOccurred);
                        } else {
                            fileSupplier.remove(file1);
                        }
                    });
                } catch (InterruptedException e) {
                    if (quit) {
                        log.info("data consumer close by itself");
                    } else {
                        log.error("data consumer interrupted!", e);
                    }
                } catch (Exception e) {
                    log.error("process data error", e);
                }
            }

            log.info("data engine[region={}] is shut down!", storageRegionName);
        }

    }

}
