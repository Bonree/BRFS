package com.bonree.brfs.rebalance.route;

import java.util.List;

import com.bonree.brfs.rebalance.task.TaskVersion;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月25日 下午5:19:02
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 2级serverID的迁移记录
 ******************************************************************************/
public class NormalRoute {
    private String changeID;
    private int storageIndex;
    private String secondID;
    private List<String> newSecondIDs;
    private TaskVersion version;

    @SuppressWarnings("unused")
    private NormalRoute() {
    }

    public NormalRoute(String changeID, int storageIndex, String secondID, List<String> newSecondIDs, TaskVersion version) {
        this.changeID = changeID;
        this.storageIndex = storageIndex;
        this.secondID = secondID;
        this.newSecondIDs = newSecondIDs;
        this.version = version;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    public String getSecondID() {
        return secondID;
    }

    public void setSecondID(String secondID) {
        this.secondID = secondID;
    }

    public List<String> getNewSecondIDs() {
        return newSecondIDs;
    }

    public void setNewSecondIDs(List<String> newSecondIDs) {
        this.newSecondIDs = newSecondIDs;
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
