package com.bonree.brfs.rebalance.recover;


public class FileRecoverMeta {

    private final String fileName;
    private final String storageName;
    private final String time;
    private final int replica;
    private final int pot;
    private final String firstServerID;

    public FileRecoverMeta(String fileName, String storageName, String time, int replica,int pot, String firstServerID) {
        super();
        this.fileName = fileName;
        this.storageName = storageName;
        this.time = time;
        this.replica = replica;
        this.pot = pot;
        this.firstServerID = firstServerID;
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

    @Override
    public String toString() {
        return "FileRecoverMeta [fileName=" + fileName + ", storageName=" + storageName + ", time=" + time + ", replica=" + replica + ", pot=" + pot + ", firstServerID=" + firstServerID + "]";
    }

}
