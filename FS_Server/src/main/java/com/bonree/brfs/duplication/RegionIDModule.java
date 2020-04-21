package com.bonree.brfs.duplication;


import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.disknode.IDConfig;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.SimplePartitionNodeSelecotr;
import com.bonree.brfs.duplication.filenode.duplicates.impl.refactor.DuplicateNodeFactory;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.identification.impl.SecondIDRelationShip;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.google.inject.*;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;

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
        binder.bind(VirtualServerID.class).to(VirtualServerIDImpl.class).in(Scopes.SINGLETON);
        LifecycleModule.register(binder,DiskPartitionInfoManager.class);
    }
    @Provides
    @Singleton
    public FirstLevelServerIDImpl getFirstLevelServerIDImpl(CuratorFramework client,
                                                            ZookeeperPaths path, IDConfig idConfig) {
        return new FirstLevelServerIDImpl(client, path.getBaseServerIdPath(), idConfig.getServerIds() + File.separator + "disknode_id", path.getBaseSequencesPath());
    }
    @Provides
    @Singleton
    public VirtualServerIDImpl getVirtualServerId(CuratorFramework client,
                                                  ZookeeperPaths path){
        return new VirtualServerIDImpl(client,path.getBaseServerIdSeqPath());
    }
    @Provides
    @Singleton
    public RouteLoader getRouteLoader(CuratorFramework client,ZookeeperPaths zookeeperPaths){
        return new SimpleRouteZKLoader(client,zookeeperPaths.getBaseRoutePath());
    }
    @Provides
    @Singleton
    public SecondIdsInterface getSecondIdsInterface(CuratorFramework client,ZookeeperPaths zookeeperPaths){
        try {
            return new SecondIDRelationShip(client,zookeeperPaths.getBaseV2SecondIDPath());
        } catch (Exception e) {
            throw new RuntimeException("create secondIds happen error !",e);
        }
    }
    @Provides
    @Singleton
    public DiskPartitionInfoManager getDiskPartitionInfoManager(ZookeeperPaths zookeeperPaths, Lifecycle lifecycle){
        DiskPartitionInfoManager manager =  new DiskPartitionInfoManager(zookeeperPaths);
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {
                manager.start();
            }

            @Override
            public void stop() {
                try {
                    manager.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return manager;
    }
    @Provides
    public PartitionNodeSelector getPartitionNodeSelecotr(DiskPartitionInfoManager diskPartitionInfoManager){
        return new SimplePartitionNodeSelecotr(diskPartitionInfoManager);
    }
    @Provides
    public DuplicateNodeSelector getDuplicateNodeSelector(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool, FileNodeStorer storer, PartitionNodeSelector pSelector, SecondIdsInterface secondIds, ZookeeperPaths zookeeperPaths, CuratorFramework client){
        try {
            return DuplicateNodeFactory.create(serviceManager,connectionPool,storer,pSelector,secondIds,zookeeperPaths,client);
        } catch (Exception e) {
            throw new RuntimeException("create duplicateNodeSelector happen error ",e);
        }
    }
}
