package com.bonree.brfs.common.resource.vo;

import java.util.Collection;

public class NodeSnapshotInfo {
    private String nodeId;
    private String groupId;
    private String host;
    private long time;
    private OSInfo os;
    private CPUInfo cpu;
    private MemorySwapInfo memSwap;
    private CpuStat cpustat;
    private MemStat memStat;
    private SwapStat swapStat;
    private Load load;
    private Collection<NetStat> netStats;
    private Collection<DiskPartitionStat> diskPartitionStats;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public OSInfo getOs() {
        return os;
    }

    public void setOs(OSInfo os) {
        this.os = os;
    }

    public CPUInfo getCpu() {
        return cpu;
    }

    public void setCpu(CPUInfo cpu) {
        this.cpu = cpu;
    }

    public MemorySwapInfo getMemSwap() {
        return memSwap;
    }

    public void setMemSwap(MemorySwapInfo memSwap) {
        this.memSwap = memSwap;
    }

    public CpuStat getCpustat() {
        return cpustat;
    }

    public void setCpustat(CpuStat cpustat) {
        this.cpustat = cpustat;
    }

    public MemStat getMemStat() {
        return memStat;
    }

    public void setMemStat(MemStat memStat) {
        this.memStat = memStat;
    }

    public SwapStat getSwapStat() {
        return swapStat;
    }

    public void setSwapStat(SwapStat swapStat) {
        this.swapStat = swapStat;
    }

    public Load getLoad() {
        return load;
    }

    public void setLoad(Load load) {
        this.load = load;
    }

    public Collection<NetStat> getNetStats() {
        return netStats;
    }

    public void setNetStats(Collection<NetStat> netStats) {
        this.netStats = netStats;
    }

    public Collection<DiskPartitionStat> getDiskPartitionStats() {
        return diskPartitionStats;
    }

    public void setDiskPartitionStats(Collection<DiskPartitionStat> diskPartitionStats) {
        this.diskPartitionStats = diskPartitionStats;
    }
}
