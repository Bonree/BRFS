package com.bonree.brfs.resource.vo;

/**
 * 集群磁盘空间大小
 */
public class ClusterStorageInfo {
    private long clustorStorageSize;
    private long clustorStorageRemainSize;

    public ClusterStorageInfo(long clustorStorageSize, long clustorStorageRemainSize) {
        this.clustorStorageSize = clustorStorageSize;
        this.clustorStorageRemainSize = clustorStorageRemainSize;
    }

    public long getClustorStorageRemainSize() {
        return clustorStorageRemainSize;
    }

    public void setClustorStorageRemainSize(long clustorStorageRemainSize) {
        this.clustorStorageRemainSize = clustorStorageRemainSize;
    }

    public long getClustorStorageSize() {
        return clustorStorageSize;
    }

    public void setClustorStorageSize(long clustorStorageSize) {
        this.clustorStorageSize = clustorStorageSize;
    }
}
