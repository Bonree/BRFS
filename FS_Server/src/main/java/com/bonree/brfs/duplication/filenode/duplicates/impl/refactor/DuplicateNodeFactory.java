package com.bonree.brfs.duplication.filenode.duplicates.impl.refactor;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MinimalDuplicateNodeSelector;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 20:37
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class DuplicateNodeFactory {
    public static DuplicateNodeSelector create(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool,
                                               FileNodeStorer storer, PartitionNodeSelector nodeSelector,
                                               SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths,
                                               CuratorFramework client) throws Exception {
        int type = Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_DUPLICATION_SELECT_TYPE);
        // 1随机，2资源
        if (type == 1) {
            return createRandom(serviceManager, connectionPool, nodeSelector, secondIds);
        } else if (type == 2) {
            return createResource(serviceManager, connectionPool, nodeSelector, secondIds, zookeeperPaths, client, storer);
        } else {
            throw new RuntimeException("[invalid config] regionnode.duplication.select.type  " + type);
        }

    }

    private static DuplicateNodeSelector createRandom(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool,
                                                      PartitionNodeSelector nodeSelector, SecondIdsInterface secondIds) {
        return new RandomSelector(serviceManager, connectionPool, nodeSelector, secondIds);
    }

    private static DuplicateNodeSelector createResource(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool,
                                                        PartitionNodeSelector nodeSelector, SecondIdsInterface secondIds,
                                                        ZookeeperPaths zookeeperPaths, CuratorFramework client,
                                                        FileNodeStorer storer) throws Exception {
        LimitServerResource limitServerResource = new LimitServerResource();
        String rpath = zookeeperPaths.getBaseResourcesPath() + "/" + limitServerResource.getDiskGroup() + "/resource";
        ClusterResource clusterResource = ClusterResource.newBuilder()
                                                         .setCache(true)
                                                         .setClient(client)
                                                         .setListenPath(rpath)
                                                         .setPool(Executors.newSingleThreadExecutor())
                                                         .build()
                                                         .start();
        MachineResourceWriterSelector serviceSelector =
            new MachineResourceWriterSelector(connectionPool, storer, limitServerResource);
        // 生成备用选择器
        DuplicateNodeSelector bakSelect = new MinimalDuplicateNodeSelector(serviceManager, connectionPool);
        return new ResourceSelector(clusterResource, serviceSelector, bakSelect, limitServerResource.getDiskGroup(), nodeSelector,
                                    secondIds);
    }
}
