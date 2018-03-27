package com.bonree.brfs.duplication.storagename;

import com.bonree.brfs.duplication.utils.LifeCycle;

public interface StorageNameManager extends LifeCycle {
	boolean exists(String storageName);
	StorageNameNode createStorageName(String storageName, int replicas, int ttl);
	boolean updateStorageName(String storageName, int ttl);
	boolean removeStorageName(int storageId);
	boolean removeStorageName(String storageName);
	StorageNameNode findStorageName(String storageName);
	StorageNameNode findStorageName(int id);
}
