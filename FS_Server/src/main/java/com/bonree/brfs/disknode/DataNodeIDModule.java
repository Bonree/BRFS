package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.identification.impl.DiskNodeIDImpl;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.identification.impl.LocalDirMaintainer;
import com.bonree.brfs.identification.impl.SimpleSecondMaintainer;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.partition.PartitionCheckingRoutine;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalancev2.task.DiskPartitionChangeTaskGenerator;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.File;
import java.util.Collection;
import org.apache.curator.framework.CuratorFramework;

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
        // 加载配置
        JsonConfigProvider.bind(binder, "partition", PartitionConfig.class);

        binder.bind(VirtualServerID.class).to(VirtualServerIDImpl.class);
        binder.bind(LocalPartitionInterface.class).to(DiskDaemon.class);
        binder.bind(SecondIdsInterface.class).to(SecondMaintainerInterface.class).in(ManageLifecycle.class);
        binder.bind(DiskPartitionInfoManager.class).in(ManageLifecycle.class);
        LifecycleModule.register(binder, IDSManager.class);
        LifecycleModule.register(binder, DiskPartitionChangeTaskGenerator.class);
    }

    @Provides
    @Singleton
    public DiskDaemon getDiskDaemon(CuratorFramework client, ZookeeperPaths zkpath, Service firstLevelServerID,
                                    StorageConfig storageConfig, PartitionConfig partitionConfig, IDConfig idConfig,
                                    ResourceCollectionInterface resourceGather, Lifecycle lifecycle) {
        // 1.生成注册id实例
        DiskNodeIDImpl diskNodeID = new DiskNodeIDImpl(client, zkpath.getBaseServerIdSeqPath(), zkpath.getBaseV2SecondIDPath());
        // 2.生成磁盘分区id检查类
        PartitionCheckingRoutine routine =
            new PartitionCheckingRoutine(diskNodeID, resourceGather, storageConfig.getStorageDirs(), idConfig.getPartitionIds(),
                                         partitionConfig.getPartitionGroupName());
        Collection<LocalPartitionInfo> parts = routine.checkVaildPartition();
        // 3.生成注册管理实例
        PartitionInfoRegister register = new PartitionInfoRegister(client, zkpath.getBaseDiscoveryPath());
        // 4.生成采集线程池
        PartitionGather gather =
            new PartitionGather(resourceGather, register, firstLevelServerID, routine.checkVaildPartition(),
                                partitionConfig.getIntervalTime());
        DiskDaemon daemon = new DiskDaemon(gather, parts);
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
                                                            ZookeeperPaths path, IDConfig idConfig) {
        return new FirstLevelServerIDImpl(client, path.getBaseServerIdPath(),
                                          idConfig.getServerIds() + File.separator + "disknode_id", path.getBaseSequencesPath());
    }

    @Provides
    @Singleton
    public VirtualServerIDImpl getVirtualServerId(CuratorFramework client,
                                                  ZookeeperPaths path) {
        return new VirtualServerIDImpl(client, path.getBaseServerIdSeqPath());
    }

    @Provides
    @Singleton
    public SecondMaintainerInterface getSecondMaintainer(CuratorFramework client, ZookeeperPaths path) {
        return new SimpleSecondMaintainer(client, path.getBaseV2SecondIDPath(), path.getBaseV2RoutePath(),
                                          path.getBaseServerIdSeqPath());
    }

    @Provides
    @Singleton
    public IDSManager getIDSManager(FirstLevelServerIDImpl firstLevelServerID,
                                    SecondMaintainerInterface ship,
                                    VirtualServerID virtualServerID,
                                    DiskDaemon diskDaemon, Lifecycle lifecycle) {
        IDSManager manager = new IDSManager(firstLevelServerID.initOrLoadServerID(), ship, virtualServerID, diskDaemon);
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
    public PartitionInterface getPartitionInterface(DiskDaemon diskDaemon, SecondMaintainerInterface secondIds) {
        return new LocalDirMaintainer(diskDaemon, secondIds);
    }

    @Provides
    @Singleton
    public DiskPartitionChangeTaskGenerator diskPartitionChangeTaskGenerator(
        CuratorFramework client,
        ServiceManager serviceManager,
        IDSManager idsManager,
        StorageRegionManager storageRegionManager,
        ZookeeperPaths zkPaths,
        DiskPartitionInfoManager diskPartitionInfoManager,
        Lifecycle lifecycle) {
        DiskPartitionChangeTaskGenerator generator = new DiskPartitionChangeTaskGenerator(client,
                                                                                          serviceManager,
                                                                                          idsManager,
                                                                                          storageRegionManager,
                                                                                          zkPaths,
                                                                                          diskPartitionInfoManager);
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {
                generator.start();
            }

            @Override
            public void stop() {
                try {
                    generator.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return generator;
    }

}
