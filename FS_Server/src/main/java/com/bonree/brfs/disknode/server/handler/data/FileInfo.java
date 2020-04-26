package com.bonree.brfs.disknode.server.handler.data;

public class FileInfo {
    public static final int TYPE_DIR = 0;
    public static final int TYPE_FILE = 1;

    private int level;
    private int type;
    private String path;

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
