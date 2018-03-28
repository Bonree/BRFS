package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午2:54:23
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 节点发生改变，包括节点丢失或者节点加入
 ******************************************************************************/

public class ChangeSummary {

    private int storageIndex;

    private long createTime;

    private ChangeType changeType;

    private int status;

    private String changeServer;

    private List<String> currentServers;
    
    private ChangeSummary() {

    }

    public ChangeSummary(int storageIndex, long createTime, ChangeType changeType, String changeServer, List<String> currentServers) {
        this.storageIndex = storageIndex;
        this.createTime = createTime;
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

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public static void main(String[] args) {
        List<String> servers = new ArrayList<>();
        servers.add("bbb");
        servers.add("ccc");
        ChangeSummary summary = new ChangeSummary(1, 15456455l, ChangeType.ADD, "aaaaa", servers);
        String jsonStr = JSON.toJSONString(summary);
        System.out.println(jsonStr);
        ChangeSummary summary1=JSON.parseObject(jsonStr,ChangeSummary.class);
        System.out.println(summary1.getChangeType() == ChangeType.ADD);
        
    }

}
