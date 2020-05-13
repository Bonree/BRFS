package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycleInit;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycleInit
public class FileObjectSupplierManager {
    private static final Logger log = LoggerFactory.getLogger(FileObjectSupplierManager.class);

    private final ConcurrentMap<String, FileObjectSupplier> suppliers = new ConcurrentHashMap<>();

    private final FileObjectSupplierFactory factory;
    private final StorageRegionManager storageRegionManager;
    private final FileObjectSupplierUpdater updater;

    @Inject
    public FileObjectSupplierManager(FileObjectSupplierFactory factory,
                                     StorageRegionManager storageRegionManager) {
        this.factory = factory;
        this.storageRegionManager = storageRegionManager;
        this.updater = new FileObjectSupplierUpdater();
    }

    public FileObjectSupplier getFileObjectSupplier(String srName) {
        return suppliers.get(srName);
    }

    @LifecycleStart
    public void init() {
        storageRegionManager.addStorageRegionStateListener(updater);
    }

    @LifecycleStop
    public void shutdown() {
        storageRegionManager.addStorageRegionStateListener(updater);

        suppliers.forEach(this::close);
        suppliers.clear();
    }

    private void close(String srName, FileObjectSupplier supplier) {
        log.info("close file object supplier of sr[%s]", srName);
        if (supplier != null) {
            try {
                supplier.close();
            } catch (IOException e) {
                log.error("close file object supplier for storageRegion[{}] error", srName, e);
            }
        }
    }


    private class FileObjectSupplierUpdater implements StorageRegionStateListener {

        @Override
        public void storageRegionAdded(StorageRegion region) {
            if (suppliers.containsKey(region.getName())) {
                log.error("No file object supplier should be bind to sr[{}], but get one", region.getName());
                return;
            }

            suppliers.computeIfAbsent(region.getName(), srName -> factory.create(region));
        }

        @Override
        public void storageRegionUpdated(StorageRegion region) {
            if (!suppliers.containsKey(region.getName())) {
                log.error("A file object supplier should have been bind to sr[{}], bit none", region.getName());
                suppliers.computeIfAbsent(region.getName(), srName -> factory.create(region));
            }
        }

        @Override
        public void storageRegionRemoved(StorageRegion region) {
            FileObjectSupplier supplier = suppliers.remove(region.getName());
            if (supplier != null) {
                close(region.getName(), supplier);
            }
        }
    }
}