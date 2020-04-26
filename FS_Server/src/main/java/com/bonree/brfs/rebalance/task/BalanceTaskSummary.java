package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月22日 下午2:40:48
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 用户进行副本平衡的model类
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
     * 任务类型，目前有多副本恢复和虚拟ID恢复
     */
    @JsonProperty("taskType")
    private RecoverType taskType;

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

    public RecoverType getTaskType() {
        return taskType;
    }

    public void setTaskType(RecoverType taskType) {
        this.taskType = taskType;
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

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public List<String> getAliveServer() {
        return aliveServer;
    }

    public void setAliveServer(List<String> aliveServer) {
        this.aliveServer = aliveServer;
    }

    public String getChangeID() {
        return changeID;
    }

    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "BalanceTaskSummary [id=" + id + ", changeID=" + changeID + ", serverId=" + serverId + ", storageIndex="
            + storageIndex + ", taskType=" + taskType + ", taskStatus=" + taskStatus + ", outputServers=" + outputServers
            + ", inputServers=" + inputServers + ", aliveServer=" + aliveServer + ", delayTime=" + delayTime + ", interval="
            + interval + "]";
    }

}
