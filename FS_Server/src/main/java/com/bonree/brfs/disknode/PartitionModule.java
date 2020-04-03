package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.guice.NodeConfig;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.identification.impl.DiskNodeIDImpl;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.partition.PartitionCheckingRoutine;
import com.bonree.brfs.partition.PartitionGather;
import com.bonree.brfs.partition.PartitionInfoRegister;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月02日 10:01:14
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘功能连接映射
 ******************************************************************************/

public class PartitionModule implements Module {
    @Override
    public void configure(Binder binder) {

    }

    @Provides
    public LevelServerIDGen getDiskNodeIDImpl(CuratorFramework client, ZookeeperPaths paths){
        // 1.创建磁盘分区id生成器
        return new DiskNodeIDImpl(client,paths.getBaseServerIdSeqPath());
    }
    @Provides
    public PartitionInfoRegister getPartitionInfoRegister(CuratorFramework client, ClusterConfig config, ZookeeperPaths paths){
        String groupBasePath = paths.getBaseDiscoveryPath()+"/"+config.getPartitionNodeGroup();
        return new PartitionInfoRegister(client,groupBasePath);
    }

    @Provides
    public PartitionCheckingRoutine getPartitionCheckingRoutine(LevelServerIDGen partitionIdGen,
                                                                StorageConfig config,
                                                                ClusterConfig clusterConfig,
                                                                String innerPath){
        String dataDir = config.getWorkDirectory();
        return new PartitionCheckingRoutine(partitionIdGen,dataDir,innerPath,clusterConfig.getPartitionNodeGroup());
    }
    @Provides
    public PartitionGather getPartitionGather(PartitionInfoRegister register,
                                              FirstLevelServerIDImpl firstLevelServerID,
                                              ClusterConfig clusterConfig,
                                              PartitionCheckingRoutine routine,
                                              NodeConfig config){
        Service service = new Service(firstLevelServerID.initOrLoadServerID(),clusterConfig.getDataNodeGroup(),config.host,config.port);
        Collection<LocalPartitionInfo> validPartitions = routine.checkVaildPartition();
        // todo 缺少配置
        return new PartitionGather(register,service,validPartitions,10);
    }
    @Provides
    @Singleton
    public DiskDaemon getDiskDaemon(PartitionGather gather,PartitionCheckingRoutine routine){
        return new DiskDaemon(gather,routine.checkVaildPartition());
    }
}
