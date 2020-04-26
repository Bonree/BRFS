package com.bonree.brfs.disknode.server.tcp.handler.data;

public class OpenFileMessage {
    private String filePath;
    private long capacity;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }
}
