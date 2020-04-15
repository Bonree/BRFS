package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigurator;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.*;
import com.bonree.brfs.partition.PartitionCheckingRoutine;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.util.Collection;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 09:57
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class DataNodeIDModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(VirtualServerID.class).to(VirtualServerIDImpl.class);
        LifecycleModule.register(binder,DiskDaemon.class);
        LifecycleModule.register(binder,IDSManager.class);
    }
    @Provides
    @Singleton
    public DiskDaemon getDiskDaemon(CuratorFramework client, ZookeeperPaths zkpath, Service  firstLevelServerID, StorageConfig storageConfig, PartitionConfig partitionConfig, IDConfig idConfig, Lifecycle lifecycle){
        // 1.生成注册id实例
        DiskNodeIDImpl diskNodeID = new DiskNodeIDImpl(client,zkpath.getBaseServerIdSeqPath());
        // 2.生成磁盘分区id检查类
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(diskNodeID,storageConfig.getStorageDirs(),idConfig.getPartitionIds(),partitionConfig.getPartitionGroupName());
        Collection<LocalPartitionInfo> parts = routine.checkVaildPartition();
        // 3.生成注册管理实例
        PartitionInfoRegister register = new PartitionInfoRegister(client,zkpath.getBaseDiscoveryPath());
        // 4.生成采集线程池
        PartitionGather gather = new PartitionGather(register,firstLevelServerID,routine.checkVaildPartition(),partitionConfig.getIntervalTime());
        DiskDaemon daemon =  new DiskDaemon(gather,parts);
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {
                daemon.start();
            }

            @Override
            public void stop() {
                daemon.stop();
            }
        });
        return daemon;
    }
    @Provides
    @Singleton
    public FirstLevelServerIDImpl getFirstLevelServerIDImpl(CuratorFramework client,
                                                            ZookeeperPaths path,IDConfig idConfig){
        return new FirstLevelServerIDImpl(client,path.getBaseServerIdPath(),idConfig.getServerIds()+ File.separator+"disknode_id",path.getBaseSequencesPath());
    }
    @Provides
    @Singleton
    public VirtualServerIDImpl getVirtualServerId(CuratorFramework client,
                                              ZookeeperPaths path){
        return new VirtualServerIDImpl(client,path.getBaseServerIdSeqPath());
    }
    @Provides
    @Singleton
    public SecondMaintainerInterface getSecondMaintainer(CuratorFramework client, ZookeeperPaths path){
        return new SimpleSecondMaintainer(client,path.getBaseV2SecondIDPath(),path.getBaseV2RoutePath(),path.getBaseServerIdSeqPath());
    }
    @Provides
    @Singleton
    public IDSManager getIDSManager(FirstLevelServerIDImpl firstLevelServerID,
                                    SecondMaintainerInterface ship,
                                    VirtualServerID virtualServerID,
                                    DiskDaemon diskDaemon,Lifecycle lifecycle){
        IDSManager manager = new IDSManager(firstLevelServerID.initOrLoadServerID(),ship,virtualServerID,diskDaemon);
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {
                manager.start();
            }

            @Override
            public void stop() {
                manager.stop();
            }
        });
        return manager;
    }
    @Provides
    @Singleton
    public PartitionInterface getPartitionInterface(DiskDaemon diskDaemon,SecondMaintainerInterface secondIds){
        return new LocalDirMaintainer(diskDaemon,secondIds);
    }
}
