package com.bonree.brfs.rebalanceV2.task;

import com.bonree.brfs.rebalance.task.ChangeType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 10:09
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class DiskPartitionChangeSummary implements Comparable<DiskPartitionChangeSummary> {

    @JsonProperty("changeID")
    private String changeID;

    @JsonProperty("storageIndex")
    private int storageIndex;

    @JsonProperty("changeType")
    private ChangeType changeType;

    @JsonProperty("changeServer")
    private String changeServer;

    @JsonProperty("changePartitionId")
    private String changePartitionId;

    @JsonProperty("currentServers")
    private List<String> currentServers;

    @JsonProperty("currentPartitionIds")
    private List<String> currentPartitionIds;

    public String getChangeID() {
        return changeID;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public String getChangeServer() {
        return changeServer;
    }

    public String getChangePartitionId() {
        return changePartitionId;
    }

    public List<String> getCurrentServers() {
        return currentServers;
    }

    public List<String> getCurrentPartitionIds() {
        return currentPartitionIds;
    }

    public DiskPartitionChangeSummary(int storageIndex, String createTime, ChangeType changeType, String changeServer, String changePartitionId, List<String> currentServers, List<String> currentPartitionIds) {
        this.storageIndex = storageIndex;
        this.changeID = createTime;
        this.changeType = changeType;
        this.changeServer = changeServer;
        this.changePartitionId = changePartitionId;
        this.currentServers = currentServers;
        this.currentPartitionIds = currentPartitionIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DiskPartitionChangeSummary cs = (DiskPartitionChangeSummary) obj;
        return changeID.equals(cs.changeID);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public int compareTo(DiskPartitionChangeSummary o) {
        if (this == o) {
            return 0;
        }
        long diff = changeID.compareTo(o.changeID);
        return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass())
                .add("changeID", changeID)
                .add("storageIndex", storageIndex)
                .add("changeType", changeType)
                .add("changeServer", changeServer)
                .add("changePartitionId", changePartitionId)
                .add("currentServers", currentServers)
                .add("currentPartitionIds", currentPartitionIds)
                .toString();
    }
}
