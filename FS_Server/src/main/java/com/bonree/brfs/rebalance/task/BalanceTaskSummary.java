package com.bonree.brfs.rebalance.task;

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

    /**
     *需要恢复的serverID;
     */
    private String serverId;

    /**
     * 需要恢复的SN
     */
    private int storageIndex;

    /**
     * 任务类型，目前有多副本恢复和虚拟ID恢复
     */
    private int taskType;

    /**
     *任务的状态，分为正常和异常；
     *正常任务，可以正常执行，异常任务不可以执行
     */
    private int taskStatus;

    /**
     *参与提供恢复数据的servers
     */
    private List<String> outputServers;

    /**
     *参与接收恢复数据的servers
     */
    private List<String> inputServers;

    /**
     *本次平衡时存活的server
     */
    private List<String> aliveServer;


    /**
     *任务何时开始,精确到秒
     */
    private long runtime;

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

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
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

    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(int taskStatus) {
        this.taskStatus = taskStatus;
    }

    public List<String> getAliveServer() {
        return aliveServer;
    }

    public void setAliveServer(List<String> aliveServer) {
        this.aliveServer = aliveServer;
    }

}
