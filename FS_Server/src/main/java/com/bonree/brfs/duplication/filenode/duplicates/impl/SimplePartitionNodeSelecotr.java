package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.partition.model.PartitionInfo;
import com.google.inject.Inject;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 23:27
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 根据磁盘剩余大小选择磁盘
 **/
public class SimplePartitionNodeSelecotr implements PartitionNodeSelector {
    private static final Logger LOG = LoggerFactory.getLogger(SimplePartitionNodeSelecotr.class);
    private DiskPartitionInfoManager diskPartitionInfoManager = null;

    @Inject
    public SimplePartitionNodeSelecotr(DiskPartitionInfoManager diskPartitionInfoManager) {
        this.diskPartitionInfoManager = diskPartitionInfoManager;
    }

    @Override
    public String getPartitionId(String firstId) {
        Map<String, PartitionInfo> map = this.diskPartitionInfoManager.getPartitionInfosByServiceId(firstId);
        // 1.若磁盘个数为空，则返回null，
        if (map == null || map.isEmpty()) {
            LOG.warn("partition cache is empty !!");
            return null;
        }
        PartitionInfo max = null;
        for (PartitionInfo partitionInfo : map.values()) {
            if (max == null) {
                max = partitionInfo;
                continue;
            }
            if (max.getFreeSize() < partitionInfo.getFreeSize()) {
                max = partitionInfo;
            }
        }
        return max == null ? null : max.getPartitionId();

    }
}
