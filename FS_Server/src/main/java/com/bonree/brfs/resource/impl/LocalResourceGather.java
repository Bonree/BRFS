package com.bonree.brfs.resource.impl;

import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.CpuStat;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import com.bonree.brfs.common.resource.vo.Load;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.resource.vo.MemStat;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.vo.ClusterStorageInfo;
import com.bonree.brfs.resource.vo.ResourceModel;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalResourceGather implements ResourceGatherInterface {
    private static final Logger LOG = LoggerFactory.getLogger(LocalResourceGather.class);
    private DiskPartitionInfoManager manager;
    private ResourceCollectionInterface gather;
    private DiskDaemon diskDaemon;
    private Service local;

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
            LOG.info("clustor storage info is invalid !!");
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
            if (stat != null) {
                diskStats.add(stat);
            }
        }
        long totalSize = diskStats.stream().mapToLong(DiskPartitionStat::getTotal).sum();
        long availSize = diskStats.stream().mapToLong(DiskPartitionStat::getAvail).sum();
        double serviceTime = diskStats.stream().mapToDouble(DiskPartitionStat::getDiskServiceTime).average().getAsDouble();
        model.setHost(local.getHost());
        model.setServerId(local.getServiceId());
        model.setCpuRate(cpuStat.getTotal());
        model.setLoad(load.getMin1Load());
        model.setMemoryRate(memStat.getUsed());
        model.setStorageRemainSize(totalSize - availSize);
        model.setStorageSize(totalSize);
        model.setDiskServiceTime(serviceTime);
        model.setClustorStorageRemainValue((double) (totalSize) / cluster.getClustorStorageSize());
        return model;
    }

    @Override
    public NodeSnapshotInfo gatherSnapshot() throws Exception {
        NodeSnapshotInfo snapshot = new NodeSnapshotInfo();
        snapshot.setGroupId(local.getServiceGroup());
        snapshot.setNodeId(local.getServiceId());
        snapshot.setHost(local.getHost() + ":" + local.getPort());
        snapshot.setOs(gather.collectOSInfo());
        snapshot.setCpu(gather.collectCPUInfo());
        snapshot.setCpustat(gather.collectCpuStat());
        snapshot.setLoad(gather.collectAverageLoad());
        snapshot.setMemSwap(gather.collectMemorySwapInfo());
        snapshot.setSwapStat(gather.collectSwapStat());
        snapshot.setMemStat(gather.collectMemStat());
        snapshot.setNetStats(gather.collectNetStats());
        Collection<LocalPartitionInfo> locals = diskDaemon.getPartitions();
        Collection<DiskPartitionStat> stats = new ArrayList<>(locals.size());
        for (LocalPartitionInfo local : locals) {
            DiskPartitionStat stat = gather.collectSinglePartitionStats(local.getDataDir());
            stat.setPartitionId(local.getPartitionId());
            stats.add(stat);
        }
        snapshot.setDiskPartitionStats(stats);
        snapshot.setTime(System.currentTimeMillis());
        LOG.info("gather time {}", TimeUtils.formatTimeStamp(snapshot.getTime(), "yyyy-MM-dd HH:mm:ss"));
        return snapshot;
    }

}
