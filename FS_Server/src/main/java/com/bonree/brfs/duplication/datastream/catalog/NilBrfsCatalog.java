package com.bonree.brfs.duplication.datastream.catalog;

import com.bonree.brfs.duplication.catalog.BrfsCatalog;
import com.bonree.brfs.duplication.catalog.Inode;

import java.util.List;

public class NilBrfsCatalog implements BrfsCatalog {

    @Override
    public boolean isUsable() {
        return false;
    }

    @Override
    public List<Inode> list(String srName, String path, int pageNo, int pageSize) {
        return null;
    }

    @Override
    public boolean isFileNode(String srName, String path) {
        return false;
    }

    @Override
    public boolean writeFid(String srName, String path, String fid) {
        return false;
    }

    @Override
    public String getFid(String srName, String path) {
        return null;
    }

    @Override
    public boolean validPath(String path) {
        return false;
    }
}
