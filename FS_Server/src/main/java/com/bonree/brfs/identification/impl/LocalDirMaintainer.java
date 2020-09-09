package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.filenode.FilePathBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.google.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月08日 15:57:49
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class LocalDirMaintainer implements PartitionInterface {
    private static Logger LOG = LoggerFactory.getLogger(LocalDirMaintainer.class);
    private LocalPartitionInterface localPartitionInterface;
    private SecondIdsInterface secondIds;
    private StorageRegionManager storageRegionManager;
    private RouteCache cache;

    @Inject
    public LocalDirMaintainer(LocalPartitionInterface localPartitionInterface,
                              SecondIdsInterface secondIds,
                              StorageRegionManager storageRegionManager, RouteCache cache) {
        this.localPartitionInterface = localPartitionInterface;
        this.secondIds = secondIds;
        this.storageRegionManager = storageRegionManager;
        this.cache = cache;
    }

    @Override
    public String getDataDir(String secondId, int storageRegionId) {
        String partitionId = secondIds.getPartitionId(secondId, storageRegionId);
        if (StringUtils.isEmpty(partitionId)) {
            LOG.warn("partition Id is null sr:[{}],second:[{}]", storageRegionId, secondId);
            return null;
        }
        String dataPaths = localPartitionInterface.getDataPaths(partitionId);
        LOG.debug("storage {} second {} partitionid:{} path {}", storageRegionId, secondId, partitionId, dataPaths);
        return dataPaths;
    }

    @Override
    public String getDataDirByFileName(String fileRelativePath, int storageRegionId) {
        File file = new File(fileRelativePath);
        String fileName = file.getName();
        Pair<String, List<String>> blockInfo = BlockAnalyzer.analyzingFileName(fileName);
        List<String> seconds = blockInfo.getSecond();
        if (seconds == null || seconds.isEmpty()) {
            LOG.warn("block[{}] analysis no secondIDs", fileName);
            return null;
        }
        BlockAnalyzer analyzer = cache.getBlockAnalyzer(storageRegionId);
        if (analyzer == null) {
            LOG.warn("StorageRegion [{}] fileblock [{}] is invalid ! and route is empty ", storageRegionId, fileName);
            return null;
        }
        String[] secondArray = analyzer.searchVaildIds(fileName);
        if (secondArray == null || secondArray.length == 0) {
            LOG.warn("StorageRegion [{}] fileblock [{}] is invalid ! route analysis is empty ", storageRegionId, fileName);
            return null;
        }
        for (String arrayEle : secondArray) {
            String path = getDataDir(arrayEle, storageRegionId);
            if (isValidPath(path, fileRelativePath)) {
                return path;
            }
        }
        LOG.warn("StorageRegion [{}] fileblock [{}] is invalid !End ", storageRegionId, fileName);
        return null;
    }

    private boolean isValidPath(String path, String fileName) {
        return path != null;
    }

    @Override
    public String getDataDirByPath(String filePath) {
        try {
            String[] paths = FilePathBuilder.parsePath(filePath);
            StorageRegion sr = storageRegionManager.findStorageRegionByName(paths[0]);
            if (sr == null) {
                throw new IllegalStateException(String.format("no storage region[%s] is found.", paths[0]));
            }
            return getDataDirByFileName(filePath, sr.getId());
        } catch (Exception e) {
            throw new IllegalStateException("invalid path [" + filePath + "]", e);
        }
    }
}
