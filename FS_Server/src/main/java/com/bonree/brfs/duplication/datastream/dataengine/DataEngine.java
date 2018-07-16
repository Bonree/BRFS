package com.bonree.brfs.duplication.datastream.dataengine;

import java.io.Closeable;

import com.bonree.brfs.duplication.storageregion.StorageRegion;

public interface DataEngine extends Closeable {
	StorageRegion getStorageRegion();
	void store(byte[] data, DataStoreCallback callback);
}
