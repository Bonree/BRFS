package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.Load;
import com.bonree.brfs.common.resource.vo.MemStat;
import com.bonree.brfs.common.resource.vo.MemorySwapInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import com.bonree.brfs.common.resource.vo.OSInfo;
import com.bonree.brfs.common.resource.vo.SwapStat;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.metrics.DiskPartition;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.vo.ClusterStorageInfo;
import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import com.bonree.brfs.resource.vo.ResourceModel;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LocalResourceGather implements ResourceGatherInterface {
    private DiskPartitionInfoManager manager;
    private ResourceCollectionInterface gather;
    private DiskDaemon diskDaemon;
    private Service local;
    private Map<String, GuiNetInfo> guiNetInfoMap = new HashMap<>();
    @Inject
    public LocalResourceGather(DiskPartitionInfoManager manager, ResourceCollectionInterface gather,
                               DiskDaemon diskDaemon, Service local) {
        this.manager = manager;
        this.gather = gather;
        this.diskDaemon = diskDaemon;
        this.local = local;
    }

    @Override
    public ResourceModel gatherClusterResource() throws Exception {
        ClusterStorageInfo cluster = manager.getClusterStoragInfo();
        if (cluster.getClustorStorageRemainSize() <= 0 || cluster.getClustorStorageSize() <= 0) {
            return null;
        }
        CpuStat cpuStat = gather.collectCpuStat();
        Load load = gather.collectAverageLoad();
        MemStat memStat = gather.collectMemStat();
        ResourceModel model = new ResourceModel();
        Collection<LocalPartitionInfo> disks = diskDaemon.getPartitions();
        Collection<DiskPartitionStat> diskStats = new ArrayList<>();
        for (LocalPartitionInfo x : disks) {
            DiskPartitionStat stat = gather.collectSinglePartitionStats(x.getDataDir());
            diskStats.add(stat);
        }
        long totalSize = diskStats.stream().mapToLong(DiskPartitionStat::getTotal).sum();
        long availSize = diskStats.stream().mapToLong(DiskPartitionStat::getAvail).sum();
        double serviceTime = diskStats.stream().mapToDouble(DiskPartitionStat::getDiskServiceTime).average().getAsDouble();
        model.setHost(local.getHost());
        model.setServerId(local.getServiceId());
        model.setCpuRate(cpuStat.getTotal());
        model.setLoad(load.getMin1Load());
        model.setMemoryRate(memStat.getUsed());
        model.setStorageRemainSize(totalSize);
        model.setStorageSize(availSize);
        model.setDiskServiceTime(serviceTime);
        model.setClustorStorageRemainValue(totalSize/cluster.getClustorStorageSize());
        return model;
    }

    @Override
    public GuiNodeInfo gatherNodeInfo() throws Exception {
        GuiNodeInfo node = new GuiNodeInfo();
        OSInfo os = gather.collectOSInfo();
        CPUInfo cpuInfo = gather.collectCPUInfo();
        MemorySwapInfo memorySwapInfo = gather.collectMemorySwapInfo();
        node.setId(local.getServiceId());
        node.setCpuBrand(cpuInfo.getVendor());
        node.setCpuCores(cpuInfo.getCoresNum());
        node.setOs(os.getOsDescription());
        node.setTotalMemSize(memorySwapInfo.getTotalMemorySize());
        return node;
    }

    @Override
    public GuiCpuInfo gatherCpuInfo() throws Exception {
        GuiCpuInfo cpu = new GuiCpuInfo();
        CpuStat stat = gather.collectCpuStat();
        cpu.setTotal(stat.getTotal());
        cpu.setIowait(stat.getWait());
        cpu.setSystem(stat.getSys());
        cpu.setUser(stat.getUser());
        cpu.setSteal(stat.getStolen());
        return cpu;
    }

    @Override
    public GuiMemInfo gahterMemInfo() throws Exception {
        GuiMemInfo mem = new GuiMemInfo();
        MemStat memStat = gather.collectMemStat();
        SwapStat swapStat = gather.collectSwapStat();
        mem.setSwapUsed(swapStat.getUsed());
        mem.setTotalUsed(memStat.getUsed());
        return mem;
    }

    @Override
    public GuiLoadInfo gatherLoadInfo() throws Exception {
        GuiLoadInfo loadInfo = new GuiLoadInfo();
        Load load = gather.collectAverageLoad();
        loadInfo.setLoad(load.getMin1Load());
        return loadInfo;
    }

    @Override
    public Collection<GuiNetInfo> gatherNetInfos() throws Exception {
        Collection<GuiNetInfo> netInfos = new ArrayList<>();
        fixNets();
        Collection<NetStat> nets = gather.collectNetStats();
        long time = System.currentTimeMillis();
        nets.stream().forEach(x -> {
            netInfos.add(packageGuiNetInfo(x, time));
        });
        return fetch(netInfos);
    }

    private Collection<GuiNetInfo> fetch(Collection<GuiNetInfo> newNet) {
        Collection<GuiNetInfo> fetchs = new ArrayList<>();
        newNet.stream().forEach(x -> {
            if (guiNetInfoMap.get(x.getNetDev()) != null) {
                GuiNetInfo old = guiNetInfoMap.get(x.getNetDev());
                double second = (x.getTime() - old.getTime()) / 1000.0;
                long rxBytes = (long) ((x.getRxBytesPs() - old.getRxBytesPs()) / second);
                long txBytes = (long) ((x.getTxBytesPs() - old.getTxBytesPs()) / second);
                GuiNetInfo fetch = new GuiNetInfo();
                fetch.setTxBytesPs(txBytes);
                fetch.setRxBytesPs(rxBytes);
                fetch.setNetDev(x.getNetDev());
                fetchs.add(fetch);

            }
            guiNetInfoMap.put(x.getNetDev(), x);
        });
        return fetchs;
    }

    private void fixNets() throws Exception {
        if (guiNetInfoMap != null && !guiNetInfoMap.isEmpty()) {
            return;
        }
        long time = System.currentTimeMillis();
        Collection<NetStat> nets = gather.collectNetStats();
        nets.stream().forEach(x -> {
            if (guiNetInfoMap.get(x.getDevName()) == null) {
                guiNetInfoMap.put(x.getDevName(), packageGuiNetInfo(x, time));
            }
        });
        Thread.sleep(1000);
    }

    private GuiNetInfo packageGuiNetInfo(NetStat stat, long time) {
        GuiNetInfo net = new GuiNetInfo();
        net.setNetDev(stat.getDevName());
        net.setTime(time);
        net.setRxBytesPs(stat.getRxBytes());
        net.setTxBytesPs(stat.getTxBytes());
        return net;
    }

    @Override
    public Collection<GuiDiskIOInfo> gatherDiskIOInfos() throws Exception {
        Collection<LocalPartitionInfo> locals = diskDaemon.getPartitions();
        if (locals == null || locals.isEmpty()) {
            return new ArrayList<>(0);
        }
        Collection<GuiDiskIOInfo> disk = new ArrayList<>();
        for (LocalPartitionInfo local : locals) {
            DiskPartitionStat stat = this.gather.collectSinglePartitionStats(local.getDataDir());
            GuiDiskIOInfo io = packageGuiDiskIO(stat, local.getPartitionId());
            disk.add(io);
        }
        return disk;
    }

    private GuiDiskIOInfo packageGuiDiskIO(DiskPartitionStat stat, String partitionId) {
        GuiDiskIOInfo ioInfo = new GuiDiskIOInfo();
        ioInfo.setDiskId(partitionId);
        ioInfo.setUsage(stat.getDiskServiceTime());
        return ioInfo;
    }

    @Override
    public Collection<GuiDiskUsageInfo> gatherDiskUsageInfos() throws Exception {
        Collection<LocalPartitionInfo> locals = diskDaemon.getPartitions();
        if (locals == null || locals.isEmpty()) {
            return new ArrayList<>(0);
        }
        Collection<GuiDiskUsageInfo> disk = new ArrayList<>();
        for (LocalPartitionInfo local : locals) {
            DiskPartitionStat stat = this.gather.collectSinglePartitionStats(local.getDataDir());
            GuiDiskUsageInfo io = packageGuiDiskUsage(stat, local.getPartitionId());
            disk.add(io);
        }
        return disk;
    }

    private GuiDiskUsageInfo packageGuiDiskUsage(DiskPartitionStat stat, String partitionId) {
        GuiDiskUsageInfo ioInfo = new GuiDiskUsageInfo();
        ioInfo.setDiskId(partitionId);
        ioInfo.setUsage(stat.getUsePercent());
        return ioInfo;
    }
}
