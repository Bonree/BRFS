package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.DiskPartitionInfo;
import com.bonree.brfs.common.resource.vo.DiskPartitionStat;
import java.util.Collection;

public interface PartitionGather extends LifeCycle {
    DiskPartitionInfo gatherDiskPartition(String dir) throws Exception;

    Collection<DiskPartitionInfo> gatherDiskPartitions() throws Exception;

    DiskPartitionStat gatherDiskPartitonStat(String dir) throws Exception;

    Collection<DiskPartitionStat> gatherDiskPartitionStats() throws Exception;
}
