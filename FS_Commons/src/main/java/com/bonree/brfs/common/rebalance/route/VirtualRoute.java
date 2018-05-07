package com.bonree.brfs.common.rebalance.route;

import com.bonree.brfs.common.rebalance.TaskVersion;

public class VirtualRoute {

    private String changeID;
    private int storageIndex;
    private String virtualID;
    private String newSecondID;
    private TaskVersion version;

    @SuppressWarnings("unused")
    private VirtualRoute() {
    }

    public VirtualRoute(String changeID, int storageIndex, String virtualID, String newSecondID, TaskVersion version) {
        this.changeID = changeID;
        this.storageIndex = storageIndex;
        this.virtualID = virtualID;
        this.newSecondID = newSecondID;
        this.version = version;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    public String getVirtualID() {
        return virtualID;
    }

    public void setVirtualID(String virtualID) {
        this.virtualID = virtualID;
    }

    public String getNewSecondID() {
        return newSecondID;
    }

    public void setNewSecondID(String newSecondID) {
        this.newSecondID = newSecondID;
    }

    public TaskVersion getVersion() {
        return version;
    }

    public void setVersion(TaskVersion version) {
        this.version = version;
    }

    public String getChangeID() {
        return changeID;
    }

    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

}
