package com.bonree.brfs.server;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月21日 下午2:40:30
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 加载zookeeper的storage到内存中
 ******************************************************************************/

public class StorageName {
    private int index;

    private String storageName;

    private long ttl;

    private int replications;

    private String description;

    private boolean recover;

    private long triggerRecoverTime;

    public String getStorageName() {
        return storageName;
    }

    public void setStorageName(String storageName) {
        this.storageName = storageName;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public int getReplications() {
        return replications;
    }

    public void setReplications(int replications) {
        this.replications = replications;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isRecover() {
        return recover;
    }

    public void setRecover(boolean recover) {
        this.recover = recover;
    }

    public long getTriggerRecoverTime() {
        return triggerRecoverTime;
    }

    public void setTriggerRecoverTime(long triggerRecoverTime) {
        this.triggerRecoverTime = triggerRecoverTime;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

}
