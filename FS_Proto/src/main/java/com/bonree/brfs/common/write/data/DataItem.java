package com.bonree.brfs.common.write.data;

public class DataItem {
    private final byte[] bytes;

    public DataItem(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

}
