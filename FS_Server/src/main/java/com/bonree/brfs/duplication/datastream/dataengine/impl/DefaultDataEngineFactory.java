package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierManager;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import javax.inject.Inject;

public class DefaultDataEngineFactory implements DataEngineFactory {

    private final DataPoolFactory dataPoolFactory;
    private final FileObjectSupplierManager fileObjectSupplierManager;
    private final DiskWriter diskWriter;

    @Inject
    public DefaultDataEngineFactory(DataPoolFactory dataPoolFactory,
                                    FileObjectSupplierManager fileObjectSupplierManager,
                                    FileObjectSupplierFactory fileSupplierFactory,
                                    DiskWriter diskWriter) {
        this.dataPoolFactory = dataPoolFactory;
        this.fileObjectSupplierManager = fileObjectSupplierManager;
        this.diskWriter = diskWriter;
    }

    @Override
    public DataEngine createDataEngine(StorageRegion storageRegion) {
        if (!storageRegion.isEnable()) {
            throw new IllegalStateException("storage region is disabled, No data engine can be created");
        }

        return new DefaultDataEngine(storageRegion,
                                     dataPoolFactory.createDataPool(),
                                     fileObjectSupplierManager.getFileObjectSupplier(storageRegion.getName()),
                                     diskWriter);
    }

}
