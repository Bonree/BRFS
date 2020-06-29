package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.duplication.storageregion.StorageRegion;
import java.io.Closeable;

public interface FileObjectSupplier extends Closeable {
    void updateStorageRegion(StorageRegion storageRegion);

    FileObject fetch(int size) throws InterruptedException;

    void recycle(FileObject file, boolean needSync);
}
