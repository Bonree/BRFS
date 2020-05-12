package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.common.write.data.DataItem;

public interface StorageRegionWriter {
    void write(String srName, DataItem[] items, StorageRegionWriteCallback callback);

    void write(String srName, byte[] data, StorageRegionWriteCallback callback);
}
