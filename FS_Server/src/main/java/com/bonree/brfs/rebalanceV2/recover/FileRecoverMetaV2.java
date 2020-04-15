package com.bonree.brfs.rebalanceV2.recover;

import com.google.common.base.MoreObjects;

public class FileRecoverMetaV2 {

    private final String filePath;
    private final String fileName;
    private final String storageName;
    private final String time;
    private final int replica;
    private final int pot;
    private final String firstServerID;
    private final String partitionPath;

    public FileRecoverMetaV2(String filePath, String fileName, String storageName, String time, int replica, int pot, String firstServerID, String partitionPath) {
        super();
        this.filePath = filePath;
        this.fileName = fileName;
        this.storageName = storageName;
        this.time = time;
        this.replica = replica;
        this.pot = pot;
        this.firstServerID = firstServerID;
        this.partitionPath = partitionPath;
    }

    public String getFileName() {
        return fileName;
    }

    public String getStorageName() {
        return storageName;
    }

    public String getTime() {
        return time;
    }

    public int getPot() {
        return pot;
    }

    public String getFirstServerID() {
        return firstServerID;
    }

    public int getReplica() {
        return replica;
    }


    public String getFilePath() {
        return filePath;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass())
                .add("fileName", fileName)
                .add("storageName", storageName)
                .add("time", time)
                .add("replica", replica)
                .add("pot", pot)
                .add("firstServerID", firstServerID)
                .add("partitionPath", partitionPath)
                .omitNullValues()
                .toString();
    }

}
