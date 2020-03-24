/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonree.brfs.duplication;

import java.util.concurrent.Executors;

import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.Lifecycle.LifeCycleObject;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.net.tcp.client.AsyncTcpClientGroup;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.IDFilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.tcp.TcpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.datastream.dataengine.impl.BlockingQueueDataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineManager;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectCloser;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectFactory;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectCloser;
import com.bonree.brfs.duplication.datastream.file.FileObjectFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.sync.DefaultFileObjectSyncProcessor;
import com.bonree.brfs.duplication.datastream.file.sync.DefaultFileObjectSynchronier;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSyncProcessor;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizer;
import com.bonree.brfs.duplication.datastream.writer.DefaultStorageRegionWriter;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.FileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MachineResourceWriterSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MinimalDuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.ResourceWriteSelector;
import com.bonree.brfs.duplication.filenode.zk.RandomFileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeStorer;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.duplication.storageregion.impl.ZkStorageRegionIdBuilder;
import com.bonree.brfs.guice.NodeConfig;
import com.bonree.brfs.guice.ServiceGroup;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

public class RegionNodeModule implements Module {
    private static final Logger log = LoggerFactory.getLogger(RegionNodeModule.class);

    @Override
    public void configure(Binder binder) {
        binder.bindConstant().annotatedWith(ServiceGroup.class).to("region_group");
        
        binder.bind(ServerIDManager.class).in(Scopes.SINGLETON);
        binder.bind(TimeExchangeEventEmitter.class).in(Scopes.SINGLETON);
        
        binder.bind(StorageRegionIdBuilder.class).to(ZkStorageRegionIdBuilder.class).in(Scopes.SINGLETON);
        binder.bind(StorageRegionManager.class).to(DefaultStorageRegionManager.class).in(Scopes.SINGLETON);
        
        binder.bind(DiskNodeConnectionPool.class).to(TcpDiskNodeConnectionPool.class).in(Scopes.SINGLETON);
        
        binder.bind(FilePathMaker.class).to(IDFilePathMaker.class).in(Scopes.SINGLETON);
        
        binder.bind(FileObjectSyncProcessor.class).to(DefaultFileObjectSyncProcessor.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectSynchronizer.class).to(DefaultFileObjectSynchronier.class).in(Scopes.SINGLETON);
        
        binder.bind(FileNodeStorer.class).to(ZkFileNodeStorer.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectFactory.class).to(DefaultFileObjectFactory.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectCloser.class).to(DefaultFileObjectCloser.class).in(Scopes.SINGLETON);
        binder.bind(FileNodeSinkManager.class).to(ZkFileNodeSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(FileNodeSinkSelector.class).toInstance(new RandomFileNodeSinkSelector());
        binder.bind(FileObjectSupplierFactory.class).to(DefaultFileObjectSupplierFactory.class).in(Scopes.SINGLETON);
        
        binder.bind(DataPoolFactory.class).to(BlockingQueueDataPoolFactory.class).in(Scopes.SINGLETON);
        binder.bind(DiskWriter.class).in(Scopes.SINGLETON);
        binder.bind(DataEngineFactory.class).to(DefaultDataEngineFactory.class).in(Scopes.SINGLETON);
        binder.bind(DataEngineManager.class).to(DefaultDataEngineManager.class).in(Scopes.SINGLETON);
        
        binder.bind(StorageRegionWriter.class).to(DefaultStorageRegionWriter.class).in(Scopes.SINGLETON);
        
        LifecycleModule.register(binder, SimpleAuthentication.class);
    }
    
    @Provides
    @Singleton
    public ZookeeperPaths getPaths(NodeConfig node, CuratorFramework zkClient) {
        return ZookeeperPaths.create(node.getClusterName(), zkClient);
    }
    
    @Provides
    @Singleton
    public SimpleAuthentication getSimpleAuthentication(CuratorFramework zkClient, ZookeeperPaths paths, Lifecycle lifecycle) {
        SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(
                paths.getBaseLocksPath(),
                zkClient);
        
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                UserModel model = simpleAuthentication.getUser("root");
                if (model == null) {
                    throw new RuntimeException("server is not initialized");
                }
            }
            
            @Override
            public void stop() {
            }
            
        }, Lifecycle.Stage.INIT);
        
        return simpleAuthentication;
    }
    
    @Provides
    @Singleton
    public Service getService(
            NodeConfig config,
            @ServiceGroup String serviceGroup,
            ServiceManager serviceManager,
            Lifecycle lifecycle) {
        Service service = new Service(config.getServiceId(), serviceGroup, config.getHost(), config.getPort());
        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                serviceManager.registerService(service);
            }
            
            @Override
            public void stop() {
                try {
                    serviceManager.unregisterService(service);
                } catch (Exception e) {
                    log.warn("unregister service[{}] error", service, e);
                }
            }
            
        }, Lifecycle.Stage.SERVER);
        
        return service;
    }
    
    @Provides
    @Singleton
    public TcpDiskNodeConnectionPool getTcpConnectionPool(
            ServiceManager serviceManager,
            ConnectionPoolConfig config,
            Lifecycle lifecycle) {
        AsyncTcpClientGroup tcpClientGroup = new AsyncTcpClientGroup(config.getWriteWorkerThreads());
        TcpDiskNodeConnectionPool connectionPool = new TcpDiskNodeConnectionPool(serviceManager, tcpClientGroup);
        
        lifecycle.addCloseable(tcpClientGroup);
        
        return connectionPool;
    }
    
    @Provides
    @Singleton
    public DuplicateNodeSelector getNodeSelector(
            CuratorFramework zkClient,
            ZookeeperPaths paths,
            ServiceManager serviceManager,
            DiskNodeConnectionPool connectionPool,
            FileNodeStorer storer,
            StorageRegionManager storageNameManager) throws Exception {
        String diskGroup = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME);
        String rPath = paths.getBaseResourcesPath()+"/"+diskGroup+"/resource";
        ClusterResource clusterResource = ClusterResource.newBuilder()
                .setCache(true)
                .setClient(zkClient)
                .setListenPath(rPath)
                .setPool(Executors.newSingleThreadExecutor())
                .build()
                .start();
        // 资源选择策略
        // 获取限制值
        double diskRemainRate = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_AVAILABLE_RATE);
        double diskForceRemainRate = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_AVAILABLE_RATE);
        double diskwriteValue = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_WRITE_SPEED);
        double diskForcewriteValue = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_WRITE_SPEED);
        long diskRemainSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_REMAIN_SIZE);
        long diskForceRemainSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_REMAIN_SIZE);

        LimitServerResource lmit = new LimitServerResource();
        lmit.setDiskRemainRate(diskRemainRate);
        lmit.setDiskWriteValue(diskwriteValue);
        lmit.setForceDiskRemainRate(diskForceRemainRate);
        lmit.setForceWriteValue(diskForcewriteValue);
        lmit.setRemainWarnSize(diskRemainSize);
        lmit.setRemainForceSize(diskForceRemainSize);
        int centSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_RESOURCE_CENT_SIZE);
        long fileSize = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY)/1024;
        MachineResourceWriterSelector serviceSelector = new MachineResourceWriterSelector(connectionPool,storer, lmit,diskGroup,fileSize,centSize);
        // 生成备用选择器
        DuplicateNodeSelector bakSelect = new MinimalDuplicateNodeSelector(serviceManager, connectionPool);
        // 选择
        DuplicateNodeSelector nodeSelector = ResourceWriteSelector.newBuilder()
                .setBakSelector(bakSelect)
                .setDaemon(clusterResource)
                .setGroupName(diskGroup)
                .setStorageRegionManager(storageNameManager)
                .setResourceSelector(serviceSelector)
                .build();
        
        return nodeSelector;
    }
}
