package com.bonree.brfs.gui.server.resource.impl;

import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.Load;
import com.bonree.brfs.common.resource.vo.MemStat;
import com.bonree.brfs.common.resource.vo.MemorySwapInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.resource.vo.OSInfo;
import com.bonree.brfs.common.resource.vo.SwapStat;
import com.bonree.brfs.gui.server.resource.ResourceHandlerInterface;
import com.bonree.brfs.gui.server.resource.vo.GuiCpuInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiLoadInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiMemInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNetInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNodeInfo;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ResourceHandler implements ResourceHandlerInterface {
    private Map<String, GuiNetInfo> guiNetInfoMap = new HashMap<>();

    @Inject
    public ResourceHandler() {
    }

    @Override
    public GuiNodeInfo gatherNodeInfo(NodeSnapshotInfo snapshot) throws Exception {
        GuiNodeInfo node = new GuiNodeInfo();
        OSInfo os = snapshot.getOs();
        CPUInfo cpuInfo = snapshot.getCpu();
        MemorySwapInfo memorySwapInfo = snapshot.getMemSwap();
        node.setId(snapshot.getNodeId());
        node.setCpuBrand(cpuInfo.getVendor());
        node.setCpuCores(cpuInfo.getCoresNum());
        node.setOs(os.getOsDescription());
        node.setTotalMemSize(memorySwapInfo.getTotalMemorySize());
        return node;
    }

    @Override
    public GuiCpuInfo gatherCpuInfo(NodeSnapshotInfo snapshot) throws Exception {
        GuiCpuInfo cpu = new GuiCpuInfo();
        CpuStat stat = snapshot.getCpustat();
        cpu.setTotal(stat.getTotal());
        cpu.setIowait(stat.getWait());
        cpu.setSystem(stat.getSys());
        cpu.setUser(stat.getUser());
        cpu.setSteal(stat.getStolen());
        cpu.setTime(snapshot.getTime());
        return cpu;
    }

    @Override
    public GuiMemInfo gahterMemInfo(NodeSnapshotInfo snapshot) throws Exception {
        GuiMemInfo mem = new GuiMemInfo();
        MemStat memStat = snapshot.getMemStat();
        SwapStat swapStat = snapshot.getSwapStat();
        mem.setSwapUsed(swapStat.getUsed());
        mem.setTotalUsed(memStat.getUsed());
        mem.setTime(snapshot.getTime());
        return mem;
    }

    @Override
    public GuiLoadInfo gatherLoadInfo(NodeSnapshotInfo snapshot) throws Exception {
        GuiLoadInfo loadInfo = new GuiLoadInfo();
        Load load = snapshot.getLoad();
        loadInfo.setLoad(load.getMin1Load());
        loadInfo.setTime(snapshot.getTime());
        return loadInfo;
    }

    @Override
    public Collection<GuiNetInfo> gatherNetInfos(NodeSnapshotInfo snapshot) throws Exception {
        Collection<GuiNetInfo> netInfos = new ArrayList<>();
        Collection<NetStat> nets = snapshot.getNetStats();
        long time = snapshot.getTime();
        nets.stream().forEach(x -> {
            netInfos.add(packageGuiNetInfo(x, time));
        });
        return fetch(snapshot.getNodeId(), snapshot.getGroupId(), netInfos);
    }

    private Collection<GuiNetInfo> fetch(String nodeId, String groupId, Collection<GuiNetInfo> newNet) {
        Collection<GuiNetInfo> fetchs = new ArrayList<>();
        newNet.stream().forEach(x -> {
            String key = StringUtils.join(groupId, ":", nodeId, ":", x.getNetDev());
            GuiNetInfo fetch = new GuiNetInfo();
            fetch.setNetDev(x.getNetDev());
            fetch.setTime(x.getTime());
            if (guiNetInfoMap.get(key) != null) {
                GuiNetInfo old = guiNetInfoMap.get(key);
                double second = (x.getTime() - old.getTime()) / 1000.0;
                if (second != 0) {
                    long rxBytes = (long) ((x.getRxBytesPs() - old.getRxBytesPs()) / second);
                    long txBytes = (long) ((x.getTxBytesPs() - old.getTxBytesPs()) / second);
                    fetch.setTxBytesPs(txBytes);
                    fetch.setRxBytesPs(rxBytes);
                } else {
                    fetch.setTxBytesPs(0);
                    fetch.setRxBytesPs(0);
                }
            } else {
                fetch.setTxBytesPs(0);
                fetch.setRxBytesPs(0);
            }
            fetchs.add(fetch);
            guiNetInfoMap.put(key, x);
        });
        return fetchs;
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
    public Collection<GuiDiskIOInfo> gatherDiskIOInfos(NodeSnapshotInfo snapshot) throws Exception {
        Collection<DiskPartitionStat> locals = snapshot.getDiskPartitionStats();
        if (locals == null || locals.isEmpty()) {
            return new ArrayList<>(0);
        }
        Collection<GuiDiskIOInfo> disk = new ArrayList<>();
        long time = snapshot.getTime();
        for (DiskPartitionStat stat : locals) {
            GuiDiskIOInfo io = packageGuiDiskIO(stat, time);
            disk.add(io);
        }
        return disk;
    }

    private GuiDiskIOInfo packageGuiDiskIO(DiskPartitionStat stat, long time) {
        GuiDiskIOInfo ioInfo = new GuiDiskIOInfo();
        ioInfo.setDiskId(stat.getPartitionId());
        ioInfo.setUsage(stat.getDiskServiceTime());
        ioInfo.setTime(time);
        return ioInfo;
    }

    @Override
    public Collection<GuiDiskUsageInfo> gatherDiskUsageInfos(NodeSnapshotInfo snapshot) throws Exception {
        Collection<DiskPartitionStat> locals = snapshot.getDiskPartitionStats();
        if (locals == null || locals.isEmpty()) {
            return new ArrayList<>(0);
        }
        long time = snapshot.getTime();
        Collection<GuiDiskUsageInfo> disk = new ArrayList<>();
        for (DiskPartitionStat stat : locals) {
            GuiDiskUsageInfo usage = packageGuiDiskUsage(stat, time);
            disk.add(usage);
        }
        return disk;
    }

    private GuiDiskUsageInfo packageGuiDiskUsage(DiskPartitionStat stat, long time) {
        GuiDiskUsageInfo usage = new GuiDiskUsageInfo();
        usage.setDiskId(stat.getPartitionId());
        usage.setUsage(stat.getUsePercent());
        usage.setTime(time);
        return usage;
    }
}
