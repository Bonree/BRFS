package com.bonree.brfs.common.resource;

import com.bonree.brfs.common.process.LifeCycle;
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

public interface ResourceCollectionInterface extends LifeCycle {
    /**
     * 采集系统基本信息
     *
     * @return
     *
     * @throws Exception
     */
    OSInfo collectOSInfo() throws Exception;

    /**
     * 采集cpu基本信息
     *
     * @return
     *
     * @throws Exception
     */
    CPUInfo collectCPUInfo() throws Exception;

    /**
     * 采集cpu使用状态
     *
     * @return
     *
     * @throws Exception
     */
    CpuStat collectCpuStat() throws Exception;

    /**
     * 采集磁盘分区基本信息
     *
     * @return
     *
     * @throws Exception
     */
    Collection<DiskPartitionInfo> collectPartitionInfos() throws Exception;

    /**
     * 采集磁盘分区状态信息
     *
     * @return
     *
     * @throws Exception
     */
    Collection<DiskPartitionStat> collectPartitionStats() throws Exception;

    /**
     * 根据挂在点/目录 获取磁盘分区基本信息
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    DiskPartitionInfo collectSinglePartitionInfo(String path) throws Exception;

    /**
     * 根据挂载点/目录 获取磁盘分区状态信息
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    DiskPartitionStat collectSinglePartitionStats(String path) throws Exception;

    /**
     * 获取 内存 swap基本信息
     *
     * @return
     *
     * @throws Exception
     */
    MemorySwapInfo collectMemorySwapInfo() throws Exception;

    /**
     * 获取内存使用状态信息
     *
     * @return
     *
     * @throws Exception
     */
    MemStat collectMemStat() throws Exception;

    /**
     * 获取swap使用状态信息
     *
     * @return
     *
     * @throws Exception
     */
    SwapStat collectSwapStat() throws Exception;

    /**
     * 获取网络设备基本信息
     *
     * @return
     *
     * @throws Exception
     */
    Collection<NetInfo> collectNetInfos() throws Exception;

    /**
     * 获取网络设备使用状态信息
     *
     * @return
     *
     * @throws Exception
     */
    Collection<NetStat> collectNetStats() throws Exception;

    /**
     * 根据ip获取网络设备基本信息
     *
     * @param ipAddress
     *
     * @return
     *
     * @throws Exception
     */
    NetInfo collectSingleNetInfo(String ipAddress) throws Exception;

    /***
     * 根据ip获取网络设备使用信息
     * @param ipAddress
     * @return
     * @throws Exception
     */
    NetStat collectSingleNetStat(String ipAddress) throws Exception;

    /**
     * 采集最近1分钟 5分钟 15分钟的负载的均值
     *
     * @return
     *
     * @throws Exception
     */
    Load collectAverageLoad() throws Exception;
}
