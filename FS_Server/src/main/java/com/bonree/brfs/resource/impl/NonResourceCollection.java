package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.Load;
import com.bonree.brfs.common.resource.vo.MemStat;
import com.bonree.brfs.common.resource.vo.MemorySwapInfo;
import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import com.bonree.brfs.common.resource.vo.OSInfo;
import com.bonree.brfs.common.resource.vo.SwapStat;
import java.util.Collection;

public class NonResourceCollection implements ResourceCollectionInterface {
    @Override
    public OSInfo collectOSInfo() throws Exception {
        return null;
    }

    @Override
    public CPUInfo collectCPUInfo() throws Exception {
        return null;
    }

    @Override
    public CpuStat collectCpuStat() throws Exception {
        return null;
    }

    @Override
    public Collection<DiskPartitionInfo> collectPartitionInfos() throws Exception {
        return null;
    }

    @Override
    public Collection<DiskPartitionStat> collectPartitionStats() throws Exception {
        return null;
    }

    @Override
    public DiskPartitionInfo collectSinglePartitionInfo(String path) throws Exception {
        return null;
    }

    @Override
    public DiskPartitionStat collectSinglePartitionStats(String path) throws Exception {
        return null;
    }

    @Override
    public MemorySwapInfo collectMemorySwapInfo() throws Exception {
        return null;
    }

    @Override
    public MemStat collectMemStat() throws Exception {
        return null;
    }

    @Override
    public SwapStat collectSwapStat() throws Exception {
        return null;
    }

    @Override
    public Collection<NetInfo> collectNetInfos() throws Exception {
        return null;
    }

    @Override
    public Collection<NetStat> collectNetStats() throws Exception {
        return null;
    }

    @Override
    public NetInfo collectSingleNetInfo(String ip) throws Exception {
        return null;
    }

    @Override
    public NetStat collectSingleNetStat(String ip) throws Exception {
        return null;
    }

    @Override
    public Load collectAverageLoad() throws Exception {
        return null;
    }
}
