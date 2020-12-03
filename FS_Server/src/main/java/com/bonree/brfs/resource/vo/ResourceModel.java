package com.bonree.brfs.resource.vo;

import com.bonree.brfs.duplication.filenode.duplicates.impl.Weightable;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResourceModel implements Weightable {
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
    private long totalSize = 0;
    /**
     * 本机硬盘剩余大小 告警
     */
    private long freeSize = 0;

    /**
     * 本集群磁盘剩余值
     */
    private double clusterFreeValue;
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

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    public long getFreeSize() {
        return freeSize;
    }

    public void setFreeSize(long freeSize) {
        this.freeSize = freeSize;
    }

    public double getClusterFreeValue() {
        return clusterFreeValue;
    }

    public void setClusterFreeValue(double clusterFreeValue) {
        this.clusterFreeValue = clusterFreeValue;
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

    @Override
    public long weight() {
        return getFreeSize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceModel that = (ResourceModel) o;
        return serverId.equals(that.serverId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId);
    }
}
