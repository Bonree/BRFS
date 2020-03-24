package com.bonree.brfs.partition;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月23日 17:31:06
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘节点信息
 ******************************************************************************/

public class PartitionInfo {
    // 一级service的服务组
    private String serviceGroup;
    // 一级service的id
    private String serviceId;
    // 磁盘分区的服务组
    private String partitionGroup;
    // 磁盘分区的id
    private String nodeId;
    // 磁盘分区的总大小
    private double totalSize;
    // 磁盘分区的剩余大小
    private double freeSize;
    // 磁盘节点分区的注册时间
    private long registerTime;

    public PartitionInfo(double freeSize) {
        this.freeSize = freeSize;
    }

    public PartitionInfo(String serviceGroup, String serviceId, String partitionGroup, String nodeId, double totalSize, double freeSize, long registerTime) {
        this.serviceGroup = serviceGroup;
        this.serviceId = serviceId;
        this.partitionGroup = partitionGroup;
        this.nodeId = nodeId;
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

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        return  builder.append("{serviceGroup='" ).append(serviceGroup )
                .append("', serviceId='" ).append(serviceId )
                .append("', partitionGroup='" ).append(partitionGroup )
                .append("', nodeId='" ).append(nodeId )
                .append("', totalSize=" ).append(totalSize)
                .append(", freeSize=" ).append(freeSize)
                .append(", registerTime=" ).append(registerTime).append("}").toString();
    }
}
