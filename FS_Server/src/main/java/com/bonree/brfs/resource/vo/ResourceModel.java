package com.bonree.brfs.resource.vo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResourceModel {
    /**
     * 本机cpu使用率，
     */
    private double cpuRate = 0.0;
    /**
     * 本机内存使用率
     */
    private double memoryRate = 0.0;
    /**
     * 本机一级serverid
     */
    private String serverId = "";
    /**
     * 本机总共存储空间
     */
    private long storageSize = 0;
    /**
     * 本机硬盘剩余大小 告警
     */
    private long storageRemainSize = 0;

    /**
     * 本集群磁盘剩余值
     */
    private double clustorStorageRemainValue;
    /**
     * 本机负载最近1分钟
     */
    private double load;
    /**
     * 磁盘等待时间，若等待的时间越长，则磁盘越忙碌
     */
    private double diskServiceTime;

    private String host = null;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public double getCpuRate() {
        return cpuRate;
    }

    public void setCpuRate(double cpuRate) {
        this.cpuRate = cpuRate;
    }

    public double getMemoryRate() {
        return memoryRate;
    }

    public void setMemoryRate(double memoryRate) {
        this.memoryRate = memoryRate;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getStorageRemainSize() {
        return storageRemainSize;
    }

    public void setStorageRemainSize(long storageRemainSize) {
        this.storageRemainSize = storageRemainSize;
    }

    public double getClustorStorageRemainValue() {
        return clustorStorageRemainValue;
    }

    public void setClustorStorageRemainValue(double clustorStorageRemainValue) {
        this.clustorStorageRemainValue = clustorStorageRemainValue;
    }

    public double getLoad() {
        return load;
    }

    public void setLoad(double load) {
        this.load = load;
    }

    public double getDiskServiceTime() {
        return diskServiceTime;
    }

    public void setDiskServiceTime(double diskServiceTime) {
        this.diskServiceTime = diskServiceTime;
    }

}
