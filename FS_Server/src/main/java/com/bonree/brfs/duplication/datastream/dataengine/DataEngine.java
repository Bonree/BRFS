package com.bonree.brfs.duplication.datastream.dataengine;

import java.io.Closeable;

import com.bonree.brfs.duplication.storagename.StorageNameNode;

public interface DataEngine extends Closeable {
	StorageNameNode getStorageRegion();
	void store(byte[] data, DataStoreCallback callback);
}
