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
package com.bonree.brfs.disknode;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.Lifecycle.LifeCycleObject;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.net.Deliver;
import com.bonree.brfs.common.net.tcp.MessageChannelInitializer;
import com.bonree.brfs.common.net.tcp.ServerConfig;
import com.bonree.brfs.common.net.tcp.TcpServer;
import com.bonree.brfs.common.net.tcp.file.FileChannelInitializer;
import com.bonree.brfs.common.net.tcp.file.ReadObjectTranslator;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderGroup;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileFormater;
import com.bonree.brfs.disknode.server.tcp.handler.CloseFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.DeleteFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.FileRecoveryMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.FlushFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.ListFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.MetadataFetchMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.OpenFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.PingPongMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.WriteFileMessageHandler;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.rebalance.RebalanceManager;
import com.bonree.brfs.rebalance.task.ServerChangeTaskGenetor;
import com.bonree.brfs.schedulers.InitTaskManager;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

public class DataNodeModule implements Module {
    private static final Logger log = LoggerFactory.getLogger(DataNodeModule.class);

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "cluster", ClusterConfig.class);
        
        binder.bind(DiskContext.class).in(Scopes.SINGLETON);
        binder.bind(FileFormater.class).to(SimpleFileFormater.class).in(Scopes.SINGLETON);
        
        binder.bind(ServerIDManager.class).in(Scopes.SINGLETON);
        binder.bind(ServiceManager.class).to(DefaultServiceManager.class).in(Scopes.SINGLETON);
        
        binder.bind(RebalanceManager.class).in(Scopes.SINGLETON);
        LifecycleModule.register(binder, RebalanceManager.class);
        
        binder.requestStaticInjection(CuratorCacheFactory.class);
        binder.requestStaticInjection(InitTaskManager.class);
        
        binder.bind(Deliver.class).toInstance(Deliver.NOOP);
        
        LifecycleModule.register(binder, Service.class);
        LifecycleModule.register(binder, ServerChangeTaskGenetor.class);
        LifecycleModule.register(binder, TcpServer.class, DataWrite.class);
        LifecycleModule.register(binder, TcpServer.class, DataRead.class);
    }

    @Provides
    @Singleton
    public ZookeeperPaths getPaths(ClusterConfig clusterConfig, CuratorFramework zkClient, Lifecycle lifecycle) {
        ZookeeperPaths paths = ZookeeperPaths.create(clusterConfig.getName(), zkClient);
        lifecycle.addAnnotatedInstance(paths);
        
        return paths;
    }
    
    @Provides
    @Singleton
    public StorageRegionManager getStorageRegionManager(
            CuratorFramework client,
            ZookeeperPaths paths,
            SecondMaintainerInterface idManager,
            Service service,
            Lifecycle lifecycle) {
        StorageRegionManager snManager = new DefaultStorageRegionManager(client, paths, null);
        snManager.addStorageRegionStateListener(new StorageRegionStateListener() {
            private final Logger log = LoggerFactory.getLogger(StorageRegionManager.class);
            
            @Override
            public void storageRegionAdded(StorageRegion node) {
                log.info("-----------StorageNameAdded--[{}]", node);
                idManager.registerSecondIds(service.getServiceId(), node.getId());
            }

            @Override
            public void storageRegionUpdated(StorageRegion node) {
            }

            @Override
            public void storageRegionRemoved(StorageRegion node) {
                log.info("-----------StorageNameRemove--[{}]", node);
                idManager.unregisterSecondIds(service.getServiceId(), node.getId());
            }
        });
        
        // because DefaultStorageRegionManager is constructed by hand,
        // it's necessary to put it in lifecycle by hand too.
        lifecycle.addAnnotatedInstance(snManager);
        
        return snManager;
    }
    
    @Provides
    @Singleton
    public Service getService(
            ClusterConfig clusterConfig,
            ServiceManager serviceManager,
            FirstLevelServerIDImpl idManager,
            Lifecycle lifecycle) {
        Service service = new Service(
                idManager.initOrLoadServerID(),
                clusterConfig.getDataNodeGroup(),
                Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST),
                Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT));
        service.setExtraPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_PORT));
        
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
    public ResourceTaskConfig getResourceTaskConfig() {
        try {
            return ResourceTaskConfig.parse();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Provides
    @Singleton
    public ServerChangeTaskGenetor get(
            ClusterConfig clusterConfig,
            CuratorFramework client,
            ServiceManager serviceManager,
            ServerIDManager idManager,
            ZookeeperPaths paths,
            StorageRegionManager storageRegionManager,
            Lifecycle lifecycle) throws Exception {
        ServerChangeTaskGenetor generator = new ServerChangeTaskGenetor(
                client,
                serviceManager,
                idManager,
                paths.getBaseRebalancePath(),
                3000,
                storageRegionManager);
        
        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                serviceManager.addServiceStateListener(clusterConfig.getDataNodeGroup(), generator);
            }
            
            @Override
            public void stop() {}
            
        });
        
        return generator;
    }
    
    @Provides
    @Singleton
    public FileWriterManager getFileWriterManager(DiskContext diskContext, Lifecycle lifecycle) {
        FileWriterManager writerManager = new FileWriterManager(new RecordCollectionManager());
        
        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                writerManager.start();
                writerManager.rebuildFileWriterbyDir(diskContext.getRootDir());
            }
            
            @Override
            public void stop() {
                writerManager.stop();
            }
            
        }, Lifecycle.Stage.SERVER);
        
        return writerManager;
    }
    
    @Provides
    @Singleton
    @DataWrite
    public TcpServer getWriteServer(
            DiskContext diskContext,
            FileFormater fileFormater,
            FileWriterManager writerManager,
            ServiceManager serviceManager,
            Lifecycle lifecycle) {
        final int TYPE_OPEN_FILE = 0;
        final int TYPE_WRITE_FILE = 1;
        final int TYPE_CLOSE_FILE = 2;
        final int TYPE_DELETE_FILE = 3;
        final int TYPE_PING_PONG = 4;
        final int TYPE_FLUSH_FILE = 5;
        final int TYPE_METADATA = 6;
        final int TYPE_LIST_FILE = 7;
        final int TYPE_RECOVER_FILE = 8;
        
        AsyncFileReaderGroup readerGroup = new AsyncFileReaderGroup(Math.min(2, Runtime.getRuntime().availableProcessors() / 2));
        
        ExecutorService threadPool = Executors.newFixedThreadPool(
                        Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_REQUEST_HANDLER_NUM),
                        new PooledThreadFactory("message_handler"));
        
        MessageChannelInitializer initializer = new MessageChannelInitializer(threadPool);
        initializer.addMessageHandler(TYPE_OPEN_FILE, new OpenFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_WRITE_FILE, new WriteFileMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_CLOSE_FILE, new CloseFileMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_DELETE_FILE, new DeleteFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_PING_PONG, new PingPongMessageHandler());
        initializer.addMessageHandler(TYPE_FLUSH_FILE, new FlushFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_METADATA, new MetadataFetchMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_LIST_FILE, new ListFileMessageHandler(diskContext));
        
        initializer.addMessageHandler(TYPE_RECOVER_FILE,
                        new FileRecoveryMessageHandler(diskContext, serviceManager, writerManager, fileFormater, readerGroup));
        
        ServerConfig config = new ServerConfig();
        config.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")));
        config.setBossThreadNums(1);
        config.setWorkerThreadNums(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_SERVER_IO_NUM));
        config.setPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT));
        config.setHost(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST));
        
        TcpServer server = new TcpServer(config, initializer);
        
        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                server.start();
            }
            
            @Override
            public void stop() {
                server.stop();
                try {
                    readerGroup.close();
                } catch (IOException e) {
                    log.warn("close reader group error", e);
                }
                
                threadPool.shutdown();
            }
            
        }, Lifecycle.Stage.SERVER);
        
        return server;
    }
    
    @Provides
    @Singleton
    @DataRead
    public TcpServer getReadServer(
            Deliver deliver,
            DiskContext diskContext,
            FileFormater fileFormater,
            Lifecycle lifecycle) {
        ServerConfig fileServerConfig = new ServerConfig();
        fileServerConfig.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")));
        fileServerConfig.setBossThreadNums(1);
        fileServerConfig.setWorkerThreadNums(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_READER_NUM));
        fileServerConfig.setPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_PORT));
        fileServerConfig.setHost(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST));
        FileChannelInitializer fileInitializer = new FileChannelInitializer(new ReadObjectTranslator() {
                
                @Override
                public long offset(long offset) {
                        return fileFormater.absoluteOffset(offset);
                }
                
                @Override
                public int length(int length) {
                        return length;
                }
                
                @Override
                public String filePath(String path) {
                        return diskContext.getConcreteFilePath(path);
                }
                
        }, deliver);
        
        TcpServer fileServer = new TcpServer(fileServerConfig, fileInitializer);
        lifecycle.addLifeCycleObject(new LifeCycleObject() {

            @Override
            public void start() throws Exception {
                fileServer.start();
            }

            @Override
            public void stop() {
                fileServer.stop();
            }

        }, Lifecycle.Stage.SERVER);
        
        return fileServer;
    }
}
