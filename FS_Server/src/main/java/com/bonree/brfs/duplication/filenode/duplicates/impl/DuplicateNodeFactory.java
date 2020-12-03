package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.file.DuplicateNodeChecker;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.resource.vo.LimitServerResource;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 20:37
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class DuplicateNodeFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DuplicateNodeFactory.class);

    public static DuplicateNodeSelector create(
        ServiceManager serviceManager, DuplicateNodeChecker checker, PartitionNodeSelector nodeSelector,
        SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths,
        CuratorFramework client, String dataGroup) throws Exception {
        int type = Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_DUPLICATION_SELECT_TYPE);
        // 1随机，2资源
        if (type == 1) {
            LOG.info("Load random service selector !!");
            return createRandom(serviceManager, nodeSelector, secondIds, dataGroup, checker);
        } else if (type == 2) {
            LOG.info("Load resource service selector !!");
            return createResource(serviceManager, nodeSelector, secondIds, zookeeperPaths, client, dataGroup, checker);
        } else {
            throw new RuntimeException("[invalid config] regionnode.duplication.select.type  " + type);
        }

    }

    private static DuplicateNodeSelector createRandom(ServiceManager serviceManager,
                                                      PartitionNodeSelector nodeSelector, SecondIdsInterface secondIds,
                                                      String dataGroup,
                                                      DuplicateNodeChecker checker) {
        return new RandomSelector(serviceManager, nodeSelector, secondIds, dataGroup, checker);
    }

    private static DuplicateNodeSelector createResource(ServiceManager serviceManager,
                                                        PartitionNodeSelector nodeSelector,
                                                        SecondIdsInterface secondIds,
                                                        ZookeeperPaths zookeeperPaths,
                                                        CuratorFramework client,
                                                        String dataGroup,
                                                        DuplicateNodeChecker checker) throws Exception {
        LimitServerResource limitServerResource = new LimitServerResource();
        // todo 把stat写死到ZookeeperPaths中
        String statPath = ZKPaths.makePath(zookeeperPaths.getBaseResourcesPath(), "stat");
        ClusterResource clusterResource = ClusterResource.newBuilder()
                                                         .setCache(true)
                                                         .setClient(client)
                                                         .setListenPath(statPath)
                                                         .setPool(Executors.newSingleThreadExecutor())
                                                         .build();
        clusterResource.start();
        ServiceSelector serviceSelector = new MachineResourceWriterSelector(limitServerResource, checker);
        // 生成备用选择器
        DuplicateNodeSelector bakSelect = new MinimalDuplicateNodeSelector(serviceManager, dataGroup, checker);
        return new ResourceSelector(clusterResource,
                                    serviceSelector,
                                    bakSelect,
                                    limitServerResource.getDiskGroup(),
                                    nodeSelector,
                                    secondIds,
                                    checker);
    }
}
