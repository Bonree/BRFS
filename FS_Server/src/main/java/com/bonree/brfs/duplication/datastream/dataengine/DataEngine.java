package com.bonree.brfs.duplication.datastream.dataengine;

import com.bonree.brfs.duplication.storageregion.StorageRegion;
import java.io.Closeable;

public interface DataEngine extends Closeable {
    StorageRegion getStorageRegion();

    void store(byte[] data, DataStoreCallback callback);
}
