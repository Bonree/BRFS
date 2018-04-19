package com.bonree.brfs.duplication.storagename;

import java.util.List;

import com.bonree.brfs.common.utils.LifeCycle;

public interface StorageNameManager extends LifeCycle {
	boolean exists(String storageName);
	StorageNameNode createStorageName(String storageName, int replicas, int ttl);
	boolean updateStorageName(String storageName, int ttl);
	boolean removeStorageName(int storageId);
	boolean removeStorageName(String storageName);
	StorageNameNode findStorageName(String storageName);
	StorageNameNode findStorageName(int id);
	List<StorageNameNode> getStorageNameNodeList();
}
