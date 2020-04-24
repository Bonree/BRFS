package com.bonree.brfs.disknode.server.tcp.handler.data;

public class FileRecoveryMessage {
    private String filePath;
    private long offset;
    private String[] sources;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String[] getSources() {
        return sources;
    }

    public void setSources(String[] sources) {
        this.sources = sources;
    }
}
