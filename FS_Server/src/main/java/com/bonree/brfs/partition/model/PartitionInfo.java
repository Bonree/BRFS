package com.bonree.brfs.partition.model;

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
    private double totalSize;
    // 磁盘分区的剩余大小
    private double freeSize;
    // 磁盘节点分区的注册时间
    private long registerTime;
    // 版本信息
    private int version=1;

    public PartitionInfo() {

    }

    public PartitionInfo(String serviceGroup, String serviceId, String partitionGroup, String partitionId, double totalSize, double freeSize, long registerTime) {
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

    public double getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(double totalSize) {
        this.totalSize = totalSize;
    }

    public double getFreeSize() {
        return freeSize;
    }

    public void setFreeSize(double freeSize) {
        this.freeSize = freeSize;
    }

    public long getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(long registerTime) {
        this.registerTime = registerTime;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        return  builder.append("{serviceGroup='" ).append(serviceGroup )
                .append("', serviceId='" ).append(serviceId )
                .append("', partitionGroup='" ).append(partitionGroup )
                .append("', nodeId='" ).append(partitionId)
                .append("', totalSize=" ).append(totalSize)
                .append(", freeSize=" ).append(freeSize)
                .append(", registerTime=" ).append(registerTime)
                .append(",version=").append(version).append("}").toString();
    }
}
