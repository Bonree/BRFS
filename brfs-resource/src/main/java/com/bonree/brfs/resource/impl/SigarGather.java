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
import com.bonree.brfs.resource.gather.CPUGather;
import com.bonree.brfs.resource.gather.LoadGather;
import com.bonree.brfs.resource.gather.MemoryGather;
import com.bonree.brfs.resource.gather.NetGather;
import com.bonree.brfs.resource.gather.PartitionGather;
import com.bonree.brfs.resource.gather.SwapGather;
import com.bonree.brfs.resource.gather.SysInfoGather;
import com.bonree.brfs.resource.gather.impl.SigarCpuGather;
import com.bonree.brfs.resource.gather.impl.SigarLoadGather;
import com.bonree.brfs.resource.gather.impl.SigarMemoryGather;
import com.bonree.brfs.resource.gather.impl.SigarNetGather;
import com.bonree.brfs.resource.gather.impl.SigarPartitionGather;
import com.bonree.brfs.resource.gather.impl.SigarSwapGather;
import com.bonree.brfs.resource.gather.impl.SigarSysInfoGather;
import com.bonree.brfs.resource.utils.LibUtils;
import com.bonree.brfs.resource.utils.SigarUtil;
import com.google.inject.Inject;
import java.io.File;
import java.net.URL;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SigarGather implements ResourceCollectionInterface {
    private static final Logger LOG = LoggerFactory.getLogger(SigarGather.class);
    private String libPath = null;
    private CPUGather cpuGather;
    private MemoryGather memoryGather;
    private NetGather netGather;
    private PartitionGather partitionGather;
    private SwapGather swapGather;
    private SysInfoGather sysInfoGather;
    private LoadGather loadGather;
    private boolean start = false;

    @Inject
    public SigarGather() throws Exception {
        libPath = libPath();
        if (StringUtils.isNotEmpty(libPath) && libPath.contains("/target/")) {
            LibUtils.loadLibraryPath(libPath);
        } else {
            SigarUtil.getPath();
        }
        start();
    }

    @Override
    public OSInfo collectOSInfo() throws Exception {
        checkStatus();
        return this.sysInfoGather.gatherOSInfo();
    }

    @Override
    public CPUInfo collectCPUInfo() throws Exception {
        checkStatus();
        return this.cpuGather.gatherCpuInfo();
    }

    @Override
    public CpuStat collectCpuStat() throws Exception {
        checkStatus();
        return this.cpuGather.gatherCpuStat();
    }

    @Override
    public Collection<DiskPartitionInfo> collectPartitionInfos() throws Exception {
        checkStatus();
        return this.partitionGather.gatherDiskPartitions();
    }

    @Override
    public Collection<DiskPartitionStat> collectPartitionStats() throws Exception {
        checkStatus();
        return this.partitionGather.gatherDiskPartitionStats();
    }

    @Override
    public DiskPartitionInfo collectSinglePartitionInfo(String path) throws Exception {
        checkStatus();
        if (!new File(path).exists()) {
            return null;
        }
        return this.partitionGather.gatherDiskPartition(path);
    }

    @Override
    public DiskPartitionStat collectSinglePartitionStats(String path) throws Exception {
        checkStatus();
        if (!new File(path).exists()) {
            return null;
        }
        return this.partitionGather.gatherDiskPartitonStat(path);
    }

    @Override
    public MemorySwapInfo collectMemorySwapInfo() throws Exception {
        checkStatus();
        MemorySwapInfo info = new MemorySwapInfo();
        info.setTotalMemorySize(this.memoryGather.gatherMemStat().getTotal());
        info.setTotalSwapSize(this.swapGather.gatherSwap().getTotal());
        return info;
    }

    @Override
    public MemStat collectMemStat() throws Exception {
        checkStatus();
        return this.memoryGather.gatherMemStat();
    }

    @Override
    public SwapStat collectSwapStat() throws Exception {
        checkStatus();
        return this.swapGather.gatherSwap();
    }

    @Override
    public Collection<NetInfo> collectNetInfos() throws Exception {
        checkStatus();
        return this.netGather.gatherNetInfos();
    }

    @Override
    public Collection<NetStat> collectNetStats() throws Exception {
        checkStatus();
        return this.netGather.gatherNetStats();
    }

    @Override
    public NetInfo collectSingleNetInfo(String ipAddress) throws Exception {
        checkStatus();
        return this.netGather.gatherNetInfo(ipAddress);
    }

    @Override
    public NetStat collectSingleNetStat(String ipAddress) throws Exception {
        checkStatus();
        return this.netGather.gatherNetStat(ipAddress);
    }

    @Override
    public Load collectAverageLoad() throws Exception {
        checkStatus();
        return loadGather.gather();
    }

    public void start() throws Exception {
        if (start) {
            return;
        }
        cpuGather = new SigarCpuGather();
        memoryGather = new SigarMemoryGather();
        netGather = new SigarNetGather();
        partitionGather = new SigarPartitionGather();
        swapGather = new SigarSwapGather();
        sysInfoGather = new SigarSysInfoGather();
        loadGather = new SigarLoadGather();
        cpuGather.start();
        memoryGather.start();
        netGather.start();
        partitionGather.start();
        swapGather.start();
        sysInfoGather.start();
        loadGather.start();
        this.start = true;
        // 加载完成后，开始采集数据，若成功则启动成功
        this.cpuGather.gatherCpuStat();
        this.memoryGather.gatherMemStat();
        this.netGather.gatherNetInfos();
        this.loadGather.gather();
        this.sysInfoGather.gatherOSInfo();
    }

    public String libPath() throws Exception {
        URL url = this.getClass().getResource("/lib");
        return url == null ? null : url.getPath();
    }

    public void stop() throws Exception {
        cpuGather.stop();
        memoryGather.stop();
        netGather.stop();
        partitionGather.stop();
        swapGather.stop();
        sysInfoGather.stop();
        loadGather.stop();
        this.start = false;
    }

    private void checkStatus() throws Exception {
        if (!this.start) {
            throw new RuntimeException("[gather error ]Need to execute start() before executing this method !!");
        }
    }

}
