package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.LocalPartitionCache;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.google.inject.Inject;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月27日 15:21:25
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
public class DiskDaemon implements LocalPartitionInterface, LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(DiskDaemon.class);

    private PartitionGather gather;
    private Collection<LocalPartitionInfo> partitions;
    private LocalPartitionCache cache = null;

    @Inject
    public DiskDaemon(PartitionGather gather, Collection<LocalPartitionInfo> partitions) {
        this.gather = gather;
        this.partitions = partitions;
        this.cache = new LocalPartitionCache(partitions);
    }

    @Override
    public String getDataPaths(String partitionId) {
        return this.cache.getDataPaths(partitionId);
    }

    @Override
    public String getPartitionId(String dataPath) {
        return this.cache.getPartitionId(dataPath);
    }

    @Override
    public Collection<String> listPartitionId() {
        return this.cache.listPartitionId();
    }

    public Collection<LocalPartitionInfo> getPartitions() {
        return this.partitions;
    }

    @Override
    public void start() {
        this.gather.start();
        LOG.info("DiskDaemon started!!!");
    }

    @Override
    public void stop() {
        this.gather.stop();
        LOG.info("DiskDaemon stoped!!!");
    }

}
