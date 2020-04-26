package com.bonree.brfs.duplication.datastream.dataengine.impl;

import com.bonree.brfs.duplication.datastream.dataengine.DataEngine;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplier;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import javax.inject.Inject;

public class DefaultDataEngineFactory implements DataEngineFactory {

    private DataPoolFactory dataPoolFactory;
    private FileObjectSupplierFactory fileSupplierFactory;
    private DiskWriter diskWriter;

    @Inject
    public DefaultDataEngineFactory(DataPoolFactory dataPoolFactory,
                                    FileObjectSupplierFactory fileSupplierFactory,
                                    DiskWriter diskWriter) {
        this.dataPoolFactory = dataPoolFactory;
        this.fileSupplierFactory = fileSupplierFactory;
        this.diskWriter = diskWriter;
    }

    @Override
    public DataEngine createDataEngine(StorageRegion storageRegion) {
        DataPool dataPool = dataPoolFactory.createDataPool();
        FileObjectSupplier provider = fileSupplierFactory.create(storageRegion);
        return new DefaultDataEngine(storageRegion, dataPool, provider, diskWriter);
    }

}
