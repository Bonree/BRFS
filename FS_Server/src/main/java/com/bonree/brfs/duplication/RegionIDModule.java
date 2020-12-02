package com.bonree.brfs.duplication;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.disknode.PartitionConfig;
import com.bonree.brfs.duplication.filenode.FileNodeStore;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.DuplicateNodeFactory;
import com.bonree.brfs.duplication.filenode.duplicates.impl.SimplePartitionNodeSelecotr;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.SecondIDRelationShip;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月07日 19:27:25
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class RegionIDModule implements Module {
    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "partition", PartitionConfig.class);
        binder.bind(VirtualServerID.class).to(VirtualServerIDImpl.class).in(Scopes.SINGLETON);
        binder.bind(DiskPartitionInfoManager.class).in(ManageLifecycle.class);
        binder.bind(PartitionNodeSelector.class).to(SimplePartitionNodeSelecotr.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public VirtualServerIDImpl getVirtualServerId(CuratorFramework client, ZookeeperPaths path) {
        return new VirtualServerIDImpl(client, path.getBaseServerIdSeqPath());
    }

    @Provides
    @Singleton
    public RouteLoader getRouteLoader(CuratorFramework client, ZookeeperPaths zookeeperPaths) {
        return new SimpleRouteZKLoader(client, zookeeperPaths.getBaseV2RoutePath());
    }

    @Provides
    @Singleton
    public SecondIdsInterface getSecondIdsInterface(CuratorFramework client, ZookeeperPaths zookeeperPaths) {
        try {
            return new SecondIDRelationShip(client, zookeeperPaths.getBaseV2SecondIDPath());
        } catch (Exception e) {
            throw new RuntimeException("create secondIds happen error !", e);
        }
    }

    @Provides
    public DuplicateNodeSelector getDuplicateNodeSelector(
            ServiceManager serviceManager, FileNodeStore storer,
            PartitionNodeSelector partitionNodeSelector,
            SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths,
            CuratorFramework client, ClusterConfig config) {
        try {
            return DuplicateNodeFactory
                    .create(serviceManager, storer, partitionNodeSelector, secondIds, zookeeperPaths, client,
                            config.getDataNodeGroup());
        } catch (Exception e) {
            throw new RuntimeException("create duplicateNodeSelector happen error ", e);
        }
    }
}
