package com.bonree.brfs.rebalance.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月23日 下午2:54:23
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 节点发生改变，包括节点丢失或者节点加入
 ******************************************************************************/

public class ChangeSummary implements Comparable<ChangeSummary> {
    @JsonProperty("changeID")
    private String changeID;

    @JsonProperty("storageIndex")
    private int storageIndex;

    @JsonProperty("changeType")
    private ChangeType changeType;

    @JsonProperty("changeServer")
    private String changeServer;

    @JsonProperty("currentServers")
    private List<String> currentServers;

    @SuppressWarnings("unused")
    private ChangeSummary() {

    }

    public ChangeSummary(int storageIndex, String createTime, ChangeType changeType, String changeServer,
                         List<String> currentServers) {
        this.storageIndex = storageIndex;
        this.changeID = createTime;
        this.changeType = changeType;
        this.changeServer = changeServer;
        this.currentServers = currentServers;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public String getChangeServer() {
        return changeServer;
    }

    public void setChangeServer(String changeServer) {
        this.changeServer = changeServer;
    }

    public List<String> getCurrentServers() {
        return currentServers;
    }

    public void setCurrentServers(List<String> currentServers) {
        this.currentServers = currentServers;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    public String getChangeID() {
        return changeID;
    }

    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ChangeSummary cs = (ChangeSummary) obj;
        return changeID.equals(cs.changeID);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public int compareTo(ChangeSummary o) {
        if (this == o) {
            return 0;
        }
        long diff = changeID.compareTo(o.changeID);
        return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
    }

    @Override
    public String toString() {
        return "ChangeSummary [changeID=" + changeID + ", storageIndex=" + storageIndex + ", changeType=" + changeType
            + ", changeServer=" + changeServer + ", currentServers=" + currentServers + "]";
    }

}
