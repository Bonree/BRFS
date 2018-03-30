package com.bonree.brfs.rebalance.task;

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

    private int storageIndex;

    private long createTime;

    private ChangeType changeType;

    private int status;

    private String changeServer;

    private List<String> currentServers;

    @SuppressWarnings("unused")
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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
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
        return createTime == cs.createTime;
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
        long diff = createTime - o.createTime;
        return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
    }
    
    

    @Override
    public String toString() {
        return "ChangeSummary [createTime=" + createTime + "]";
    }

    public static void main(String[] args) {
//        long createTime1 = 123456798l;
//        long createTime2 = 123456789l;
//        ChangeSummary cs1 = new ChangeSummary();
//        cs1.setStorageIndex(1);
//        cs1.setCreateTime(createTime1);
//        
//        ChangeSummary cs2 = new ChangeSummary();
//        
//        cs2.setStorageIndex(2);
//        cs2.setCreateTime(createTime2);
//        List<ChangeSummary> css = new ArrayList<ChangeSummary>();
//        css.add(cs2);
//        css.add(cs1);
//        Collections.sort(css);
//        System.out.println(css);
    }

}
