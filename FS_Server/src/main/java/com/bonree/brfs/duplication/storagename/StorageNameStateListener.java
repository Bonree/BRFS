package com.bonree.brfs.duplication.storagename;

public interface StorageNameStateListener {
	void storageNameAdded(StorageNameNode node);
	void storageNameUpdated(StorageNameNode node);
	void storageNameRemoved(StorageNameNode node);
}
