package com.bonree.brfs.partition.model;

import com.google.common.base.MoreObjects;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月24日 11:10:49
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class PartitionInfo {
    // 一级service的服务组
    private String serviceGroup;
    // 一级service的id
    private String serviceId;
    // 磁盘分区的服务组
    private String partitionGroup;
    // 磁盘分区的id
    private String partitionId;
    // 磁盘分区的总大小
    private long totalSize;
    // 磁盘分区的剩余大小
    private long freeSize;
    // 磁盘节点分区的注册时间
    private long registerTime;
    // 磁盘服务时间
    private double serviceTime;
    // 版本信息
    private int version = 1;
    // 节点状态信息
    private PartitionType type = PartitionType.NORMAL;

    public PartitionInfo() {

    }

    public PartitionInfo(String serviceGroup, String serviceId, String partitionGroup, String partitionId, long totalSize,
                         long freeSize, long registerTime) {
        this.serviceGroup = serviceGroup;
        this.serviceId = serviceId;
        this.partitionGroup = partitionGroup;
        this.partitionId = partitionId;
        this.totalSize = totalSize;
        this.freeSize = freeSize;
        this.registerTime = registerTime;
    }

    public String getServiceGroup() {
        return serviceGroup;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getPartitionGroup() {
        return partitionGroup;
    }

    public void setPartitionGroup(String partitionGroup) {
        this.partitionGroup = partitionGroup;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
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

    public long getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(long registerTime) {
        this.registerTime = registerTime;
    }

    public double getServiceTime() {
        return serviceTime;
    }

    public void setServiceTime(double serviceTime) {
        this.serviceTime = serviceTime;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public PartitionType getType() {
        return type;
    }

    public void setType(PartitionType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("serviceGroup", serviceGroup)
                          .add("serviceId", serviceId)
                          .add("partitionGroup", partitionGroup)
                          .add("partitionId", partitionId)
                          .add("totalSize", totalSize)
                          .add("freeSize", freeSize)
                          .add("registerTime", registerTime)
                          .add("serviceTime", serviceTime)
                          .add("version", version)
                          .add("type", type)
                          .toString();
    }
}
