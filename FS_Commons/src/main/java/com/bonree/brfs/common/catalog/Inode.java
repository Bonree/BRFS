package com.bonree.brfs.common.catalog;

import com.bonree.brfs.common.proto.FileDataProtos;

public class Inode {
    private String storageRegion;
    private long parentID;
    private String name;
    private String fid;
    private InodeType nodeType;

    public Inode(String storageRegion, long parentID, String name, String fid, InodeType nodeType) {
        this.storageRegion = storageRegion;
        this.parentID = parentID;
        this.name = name;
        this.fid = fid;
        this.nodeType = nodeType;
    }

    public String getStorageRegion() {
        return storageRegion;
    }

    public void setStorageRegion(String storageRegion) {
        this.storageRegion = storageRegion;
    }

    public long getParentID() {
        return parentID;
    }

    public void setParentID(long parentID) {
        this.parentID = parentID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }
}
