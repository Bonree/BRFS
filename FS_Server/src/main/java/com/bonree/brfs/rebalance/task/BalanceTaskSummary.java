package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.rebalance.DataRecover;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/8 10:14
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class BalanceTaskSummary {

    @JsonProperty("id")
    private String id;

    /**
     * 和每个change进行关联
     */
    @JsonProperty("changeID")
    private String changeID;

    /**
     * 需要恢复的serverID;
     */
    @JsonProperty("serverId")
    private String serverId;

    /**
     * 需要恢复的SN
     */
    @JsonProperty("storageIndex")
    private int storageIndex;

    /**
     * @description: 需要恢复的分区
     */
    @JsonProperty("partitionId")
    private String partitionId;

    /**
     * 任务类型，目前有多副本恢复和虚拟ID恢复
     */
    @JsonProperty("taskType")
    private DataRecover.RecoverType taskType;

    /**
     * 任务的状态，分为正常和异常；
     * 正常任务，可以正常执行，异常任务不可以执行
     */
    @JsonProperty("taskStatus")
    private TaskStatus taskStatus;

    /**
     * 参与提供恢复数据的servers
     */
    @JsonProperty("outputServers")
    private List<String> outputServers;

    /**
     * 参与接收恢复数据的servers
     */
    @JsonProperty("inputServers")
    private List<String> inputServers;

    /**
     * 装载二级serverId以及对应的磁盘剩余空间容量
     */
    private Map<String, Integer> newSecondIds;

    /**
     * 二级serverid与一级server的对应关系
     */
    private Map<String, String> secondFirstShip;

    /**
     * 本次平衡时存活的server
     */
    @JsonProperty("aliveServer")
    private List<String> aliveServer;

    /**
     * 任务延迟执行时间,单位：秒
     */
    @JsonProperty("delayTime")
    private long delayTime;

    /**
     * 用于初始化倒计时间隔
     */
    @JsonProperty("interval")
    private int interval = -1;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getChangeID() {
        return changeID;
    }

    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public DataRecover.RecoverType getTaskType() {
        return taskType;
    }

    public void setTaskType(DataRecover.RecoverType taskType) {
        this.taskType = taskType;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public List<String> getOutputServers() {
        return outputServers;
    }

    public void setOutputServers(List<String> outputServers) {
        this.outputServers = outputServers;
    }

    public List<String> getInputServers() {
        return inputServers;
    }

    public void setInputServers(List<String> inputServers) {
        this.inputServers = inputServers;
    }

    public Map<String, Integer> getNewSecondIds() {
        return newSecondIds;
    }

    public void setNewSecondIds(Map<String, Integer> newSecondIds) {
        this.newSecondIds = newSecondIds;
    }

    public Map<String, String> getSecondFirstShip() {
        return secondFirstShip;
    }

    public void setSecondFirstShip(Map<String, String> secondFirstShip) {
        this.secondFirstShip = secondFirstShip;
    }

    public List<String> getAliveServer() {
        return aliveServer;
    }

    public void setAliveServer(List<String> aliveServer) {
        this.aliveServer = aliveServer;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("id", id)
                          .add("changeID", changeID)
                          .add("serverId", serverId)
                          .add("storageIndex", storageIndex)
                          .add("partitionId", partitionId)
                          .add("taskType", taskType)
                          .add("taskStatus", taskStatus)
                          .add("outputServers", outputServers)
                          .add("inputServers", inputServers)
                          .add("newSecondIds", newSecondIds)
                          .add("secondFirstShip", secondFirstShip)
                          .add("aliveServer", aliveServer)
                          .add("delayTime", delayTime)
                          .add("interval", interval)
                          .toString();
    }
}
