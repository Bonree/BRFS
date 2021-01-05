package com.bonree.brfs.disknode;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.DataNodeMetaMaintainerInterface;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.DataNodeMetaMaintainer;
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
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.route.impl.RouteParserCache;
import com.bonree.brfs.rebalance.task.DiskPartitionChangeTaskGenerator;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
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
        jaxrs(binder).resource(NodeStatResource.class);
        jaxrs(binder).resource(TrashRecoveryResource.class);
        binder.bind(VirtualServerID.class).to(VirtualServerIDImpl.class).in(Singleton.class);
        binder.bind(LocalPartitionInterface.class).to(DiskDaemon.class).in(Singleton.class);
        binder.bind(IDSManager.class).in(Singleton.class);
        binder.bind(PartitionInterface.class).to(LocalDirMaintainer.class).in(Singleton.class);
        binder.bind(FirstLevelServerIDImpl.class).in(Singleton.class);
        binder.bind(SecondMaintainerInterface.class).to(SimpleSecondMaintainer.class).in(Singleton.class);
        binder.bind(SecondIdsInterface.class).to(SimpleSecondMaintainer.class).in(Singleton.class);
        binder.bind(RouteCache.class).to(RouteParserCache.class).in(Singleton.class);
        binder.bind(DataNodeMetaMaintainerInterface.class).to(DataNodeMetaMaintainer.class).in(Singleton.class);

        binder.bind(DiskPartitionInfoManager.class).in(ManageLifecycle.class);
        binder.bind(SimpleSecondMaintainer.class).in(ManageLifecycle.class);
        binder.bind(DiskPartitionChangeTaskGenerator.class).in(ManageLifecycle.class);
        binder.bind(RouteParserCache.class).in(ManageLifecycle.class);

        LifecycleModule.register(binder, DiskPartitionInfoManager.class);
        LifecycleModule.register(binder, SimpleSecondMaintainer.class);
        LifecycleModule.register(binder, DiskPartitionChangeTaskGenerator.class);
        LifecycleModule.register(binder, DiskDaemon.class);
        LifecycleModule.register(binder, RouteParserCache.class);
    }

    @Provides
    @Singleton
    public DiskDaemon getDiskDaemon(CuratorFramework client, ZookeeperPaths zkpath, Service local,
                                    StorageConfig storageConfig, PartitionConfig partitionConfig,
                                    ResourceCollectionInterface resourceGather, Lifecycle lifecycle,
                                    SecondMaintainerInterface maintainer, DataNodeMetaMaintainerInterface metaMaintainer) {

        // 1.生成注册id实例
        DiskNodeIDImpl diskNodeID = new DiskNodeIDImpl(client, zkpath);
        // 2.生成磁盘分区id检查类
        PartitionCheckingRoutine routine = new PartitionCheckingRoutine(diskNodeID,
                                                                        resourceGather,
                                                                        storageConfig.getStorageDirs(),
                                                                        metaMaintainer, partitionConfig.getPartitionGroupName());
        Collection<LocalPartitionInfo> parts = routine.checkValidPartition();
        // 3.生成注册管理实例
        PartitionInfoRegister register = new PartitionInfoRegister(client, zkpath.getBaseDiscoveryPath());
        // 4.生成采集线程池
        PartitionGather gather =
            new PartitionGather(resourceGather, register, local, routine.checkValidPartition(),
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
        // 检测二级serverid是否要更新
        maintainer.checkSecondIds(local);
        return daemon;
    }
}
