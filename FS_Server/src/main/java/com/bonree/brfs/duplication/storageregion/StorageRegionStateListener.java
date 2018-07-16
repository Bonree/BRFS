package com.bonree.brfs.duplication.storageregion;

public interface StorageRegionStateListener {
	void storageRegionAdded(StorageRegion region);
	void storageRegionUpdated(StorageRegion region);
	void storageRegionRemoved(StorageRegion region);
}
