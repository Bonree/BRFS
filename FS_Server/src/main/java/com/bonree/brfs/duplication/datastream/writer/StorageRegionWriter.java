package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.common.write.data.DataItem;

public interface StorageRegionWriter {
	void write(int storageRegionId, DataItem[] items, StorageRegionWriteCallback callback);
	void write(int storageRegionId, byte[] data, StorageRegionWriteCallback callback);
}
