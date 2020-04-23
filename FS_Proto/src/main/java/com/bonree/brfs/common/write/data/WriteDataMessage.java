package com.bonree.brfs.common.write.data;

public class WriteDataMessage {
    private int storageNameId;
    private DataItem[] items;

    public int getStorageNameId() {
        return storageNameId;
    }

    public void setStorageNameId(int storageNameId) {
        this.storageNameId = storageNameId;
    }

    public DataItem[] getItems() {
        return items;
    }

    public void setItems(DataItem[] items) {
        this.items = items;
    }
}
