package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplier;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierManager;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import javax.inject.Inject;

public class DefaultDataEngineFactory implements DataEngineFactory {

    private final DataPoolFactory dataPoolFactory;
    private final FileObjectSupplierManager fileObjectSupplierManager;
    private final DiskWriter diskWriter;
    private final StorageRegionManager storageRegionManager;

    @Inject
    public DefaultDataEngineFactory(DataPoolFactory dataPoolFactory,
                                    FileObjectSupplierManager fileObjectSupplierManager,
                                    FileObjectSupplierFactory fileSupplierFactory,
                                    DiskWriter diskWriter,
                                    StorageRegionManager storageRegionManager) {
        this.dataPoolFactory = dataPoolFactory;
        this.fileObjectSupplierManager = fileObjectSupplierManager;
        this.diskWriter = diskWriter;
        this.storageRegionManager = storageRegionManager;
    }

    @Override
    public DataEngine createDataEngine(StorageRegion storageRegion) {
        if (!storageRegion.isEnable()) {
            throw new IllegalStateException(
                String.format("storage region[%s] is disabled, No data engine can be created", storageRegion.getName()));
        }

        FileObjectSupplier supplier = fileObjectSupplierManager.getFileObjectSupplier(storageRegion.getName());
        if (supplier == null) {
            throw new IllegalStateException(
                String.format("can not create file Object supplier for sr[%s]", storageRegion));
        }

        return new DefaultDataEngine(storageRegion.getName(),
                                     storageRegionManager,
                                     dataPoolFactory.createDataPool(),
                                     supplier,
                                     diskWriter);
    }

}
