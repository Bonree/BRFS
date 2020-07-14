package com.bonree.brfs.duplication.catalog;

import java.util.List;

public interface BrfsCatalog {
    boolean isUsable();

    List<String> getFidsByDir(String srName, String path);

    List<Inode> list(String srName, String path, int pageNo, int pageSize);

    boolean isFileNode(String srName, String path);

    boolean validPath(String path);

    default boolean writeFid(String srName, String path, String fid) {
        return true;
    }

    String getFid(String srName, String path);
}
