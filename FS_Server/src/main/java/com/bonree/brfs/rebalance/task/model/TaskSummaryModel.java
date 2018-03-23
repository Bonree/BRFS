package com.bonree.brfs.rebalance.task.model;

import java.util.List;

import com.bonree.brfs.rebalance.task.ServerChangeType;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午2:54:23
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 节点发生改变，包括节点丢失或者节点加入
 ******************************************************************************/

public class TaskSummaryModel {

    private int storageIndex;

    private long createTime;

    private ServerChangeType changeType;

    private String changeServer;

    private List<String> currentServers;

    public ServerChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ServerChangeType changeType) {
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

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
}
