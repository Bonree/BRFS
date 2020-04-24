package com.bonree.brfs.disknode.server.tcp.handler.data;

public class ListFileMessage {
    private String path;
    private int level;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }
}
