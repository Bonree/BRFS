package com.bonree.brfs.identification.impl;

import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月08日 15:57:49
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class LocalDirMaintainer implements PartitionInterface {
    private LocalPartitionInterface localPartitionInterface;
    private SecondIdsInterface secondIds;

    @Inject
    public LocalDirMaintainer(LocalPartitionInterface localPartitionInterface, SecondIdsInterface secondIds) {
        this.localPartitionInterface = localPartitionInterface;
        this.secondIds = secondIds;
    }

    @Override
    public String getDataDir(String secondId, int storageRegionId) {
        String partitionId = secondIds.getPartitionId(secondId, storageRegionId);
        if (StringUtils.isEmpty(partitionId)) {
            return null;
        }
        return localPartitionInterface.getDataPaths(partitionId);
    }
}
