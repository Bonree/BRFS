package com.bonree.brfs.duplication.filenode.duplicates.impl.refactor;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.tcp.TcpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MinimalDuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.ResourceWriteSelector;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executors;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 20:37
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class DuplicateNodeFactory {
    public static DuplicateNodeSelector create(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool, PartitionNodeSelector pSelector, SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths, CuratorFramework client, FileNodeStorer storer, StorageRegionManager storageNameManager)throws Exception{
        int type = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_DUPLICATION_SELECT_TYPE);
        // 1随机，2资源
        if(type == 1){
            return createRandom(serviceManager,connectionPool,pSelector,secondIds);
        }else if(type == 2){
            return createResource(serviceManager,connectionPool,pSelector,secondIds,zookeeperPaths,client,storer,storageNameManager);
        }else{
            throw new RuntimeException("[invalid config] regionnode.duplication.select.type  "+type);
        }

    }
    private static DuplicateNodeSelector createRandom(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool, PartitionNodeSelector pSelector, SecondIdsInterface secondIds){
        return new RandomSelector(serviceManager, connectionPool, pSelector, secondIds);
    }
    private static DuplicateNodeSelector createResource(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool, PartitionNodeSelector pSelector, SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths, CuratorFramework client, FileNodeStorer storer, StorageRegionManager storageNameManager)throws Exception{
        LimitServerResource limitServerResource = new LimitServerResource();
        String rPath = zookeeperPaths.getBaseResourcesPath() + "/" + limitServerResource.getDiskGroup() + "/resource";
        ClusterResource clusterResource = ClusterResource.newBuilder()
                .setCache(true)
                .setClient(client)
                .setListenPath(rPath)
                .setPool(Executors.newSingleThreadExecutor())
                .build()
                .start();
        MachineResourceWriterSelector serviceSelector = new MachineResourceWriterSelector(connectionPool, storer, limitServerResource);
        // 生成备用选择器
        DuplicateNodeSelector bakSelect = new MinimalDuplicateNodeSelector(serviceManager, connectionPool);
        // 选择
        DuplicateNodeSelector nodeSelector = ResourceWriteSelector.newBuilder()
                .setBakSelector(bakSelect)
                .setDaemon(clusterResource)
                .setGroupName(limitServerResource.getDiskGroup())
                .setStorageRegionManager(storageNameManager)
                .setResourceSelector(serviceSelector)
                .build();
        return new ResourceSelector(clusterResource,serviceSelector,storageNameManager,bakSelect,limitServerResource.getDiskGroup(),pSelector,secondIds);
    }
}
