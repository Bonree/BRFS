package com.bonree.brfs.rebalance.recover;

import com.bonree.brfs.rebalance.record.SimpleRecordWriter;

public class FileRecoverMeta {

    private final String fileName;
    private final String storageName;
    private final String time;
    private final int replica;
    private final int pot;
    private final String firstServerID;
    private final SimpleRecordWriter simpleWriter;

    public FileRecoverMeta(String fileName, String storageName, String time, int replica,int pot, String firstServerID, SimpleRecordWriter simpleWriter) {
        super();
        this.fileName = fileName;
        this.storageName = storageName;
        this.time = time;
        this.replica = replica;
        this.pot = pot;
        this.firstServerID = firstServerID;
        this.simpleWriter = simpleWriter;
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
    

    public SimpleRecordWriter getSimpleWriter() {
        return simpleWriter;
    }
    
    public int getReplica() {
        return replica;
    }

    @Override
    public String toString() {
        return "FileRecoverMeta [fileName=" + fileName + ", storageName=" + storageName + ", time=" + time + ", replica=" + replica + ", pot=" + pot + ", firstServerID=" + firstServerID + ", simpleWriter=" + simpleWriter + "]";
    }

}
