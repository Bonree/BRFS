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

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.jackson.JsonMapper;
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
import com.bonree.brfs.common.statistic.ReadStatCollector;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
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
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeStorer;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalancev2.RebalanceManagerV2;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeModule implements Module {
    private static final Logger log = LoggerFactory.getLogger(DataNodeModule.class);

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "cluster", ClusterConfig.class);
        JsonConfigProvider.bind(binder, "datanode.ids", IDConfig.class);
        JsonConfigProvider.bind(binder, "datanode", StorageConfig.class);

        binder.bind(DiskContext.class).in(Scopes.SINGLETON);
        binder.bind(FileFormater.class).to(SimpleFileFormater.class).in(Scopes.SINGLETON);

        binder.bind(ServiceManager.class).to(DefaultServiceManager.class).in(Scopes.SINGLETON);

        binder.bind(FileNodeStorer.class).to(ZkFileNodeStorer.class).in(Scopes.SINGLETON);

        binder.requestStaticInjection(CuratorCacheFactory.class);

        binder.bind(Deliver.class).toInstance(Deliver.NOOP);

        jaxrs(binder).resource(JsonMapper.class);
        binder.bind(ReadStatCollector.class).toInstance(new ReadStatCollector());
        jaxrs(binder).resource(StatResource.class);
        LifecycleModule.register(binder, Service.class);
        LifecycleModule.register(binder, RebalanceManagerV2.class);
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
        log.info("register storage region listener for server id");
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
            Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_HOST),
            Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_PORT));
        service.setExtraPort(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_FILE_PORT));

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
    public RebalanceManagerV2 rebalanceManagerV2(
        ZookeeperPaths zkPaths,
        IDSManager idsManager,
        StorageRegionManager storageRegionManager,
        ServiceManager serviceManager,
        LocalPartitionInterface localPartitionInterface,
        DiskPartitionInfoManager diskPartitionInfoManager,
        Lifecycle lifecycle) {
        RebalanceManagerV2 rebalanceManagerV2 = new RebalanceManagerV2(zkPaths,
                                                                       idsManager,
                                                                       storageRegionManager,
                                                                       serviceManager,
                                                                       localPartitionInterface,
                                                                       diskPartitionInfoManager);
        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            @Override
            public void start() throws Exception {
                rebalanceManagerV2.start();
            }

            @Override
            public void stop() {
                try {

                    rebalanceManagerV2.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        return rebalanceManagerV2;
    }

    @Provides
    @Singleton
    public FileWriterManager getFileWriterManager(DiskContext diskContext,
                                                  FileNodeStorer fileNodeStorer,
                                                  FileFormater fileFormater,
                                                  Lifecycle lifecycle) {
        FileWriterManager writerManager = new FileWriterManager(new RecordCollectionManager(),
                                                                fileNodeStorer,
                                                                fileFormater);

        lifecycle.addLifeCycleObject(new LifeCycleObject() {

            @Override
            public void start() throws Exception {
                writerManager.start();
                diskContext.getStorageDirs().forEach(writerManager::rebuildFileWriterbyDir);
            }

            @Override
            public void stop() {
                writerManager.stop();
            }

        }, Lifecycle.Stage.SERVER);

        return writerManager;
    }

    private static final int TYPE_OPEN_FILE = 0;
    private static final int TYPE_WRITE_FILE = 1;
    private static final int TYPE_CLOSE_FILE = 2;
    private static final int TYPE_DEL_FILE = 3;
    private static final int TYPE_PING_PONG = 4;
    private static final int TYPE_FLUSH_FILE = 5;
    private static final int TYPE_META_DATA = 6;
    private static final int TYPE_LIST_FILE = 7;
    private static final int TYPE_RECOVER_FILE = 8;

    @Provides
    @Singleton
    @DataWrite
    public TcpServer getWriteServer(
        DiskContext diskContext,
        FileFormater fileFormater,
        FileWriterManager writerManager,
        ServiceManager serviceManager,
        Lifecycle lifecycle) {
        AsyncFileReaderGroup readerGroup = new AsyncFileReaderGroup(Math.min(2, Runtime.getRuntime().availableProcessors() / 2));

        ExecutorService threadPool = Executors.newFixedThreadPool(
            Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_REQUEST_HANDLER_NUM),
            new PooledThreadFactory("message_handler"));

        MessageChannelInitializer initializer = new MessageChannelInitializer(threadPool);
        initializer.addMessageHandler(TYPE_OPEN_FILE, new OpenFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_WRITE_FILE, new WriteFileMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_CLOSE_FILE, new CloseFileMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_DEL_FILE, new DeleteFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_PING_PONG, new PingPongMessageHandler());
        initializer.addMessageHandler(TYPE_FLUSH_FILE, new FlushFileMessageHandler(diskContext, writerManager));
        initializer.addMessageHandler(TYPE_META_DATA, new MetadataFetchMessageHandler(diskContext, writerManager, fileFormater));
        initializer.addMessageHandler(TYPE_LIST_FILE, new ListFileMessageHandler(diskContext));

        initializer.addMessageHandler(TYPE_RECOVER_FILE,
                                      new FileRecoveryMessageHandler(diskContext, serviceManager, writerManager, fileFormater,
                                                                     readerGroup));

        ServerConfig config = new ServerConfig();
        config.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")));
        config.setBossThreadNums(1);
        config.setWorkerThreadNums(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_SERVER_IO_NUM));
        config.setPort(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_PORT));
        config.setHost(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_HOST));

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
        Lifecycle lifecycle,
        ReadStatCollector readStatCollector) {
        ServerConfig fileServerConfig = new ServerConfig();
        fileServerConfig.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")));
        fileServerConfig.setBossThreadNums(1);
        fileServerConfig.setWorkerThreadNums(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_FILE_READER_NUM));
        fileServerConfig.setPort(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_FILE_PORT));
        fileServerConfig.setHost(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_HOST));
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

        }, deliver, readStatCollector);

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
