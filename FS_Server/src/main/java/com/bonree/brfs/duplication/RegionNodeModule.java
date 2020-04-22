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

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.http.HttpServerConfig;
import com.bonree.brfs.common.jackson.JsonMapper;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.Lifecycle.LifeCycleObject;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.net.Deliver;
import com.bonree.brfs.common.net.tcp.client.AsyncTcpClientGroup;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.utils.NetworkUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.catalog.BrfsCatalog;
import com.bonree.brfs.duplication.catalog.DefaultBrfsCatalog;
import com.bonree.brfs.duplication.catalog.NonRocksDBManager;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.IDFilePathMaker;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManager;
import com.bonree.brfs.duplication.datastream.blockcache.BlockPool;
import com.bonree.brfs.duplication.datastream.blockcache.SeqBlockManagerV2;
import com.bonree.brfs.duplication.datastream.blockcache.SeqBlockPool;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.tcp.TcpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.bonree.brfs.duplication.datastream.dataengine.impl.BlockingQueueDataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineManager;
import com.bonree.brfs.duplication.datastream.file.*;
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
import com.bonree.brfs.duplication.filenode.zk.RandomFileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeStorer;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.metadata.MetadataBackupServer;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

public class RegionNodeModule implements Module {
    private static final Logger log = LoggerFactory.getLogger(RegionNodeModule.class);

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "cluster", ClusterConfig.class);

        binder.bind(ServiceManager.class).to(DefaultServiceManager.class);

        binder.bind(ServerIDManager.class).in(Scopes.SINGLETON);
        binder.bind(TimeExchangeEventEmitter.class);

        binder.bind(DiskNodeConnectionPool.class).to(TcpDiskNodeConnectionPool.class).in(Scopes.SINGLETON);

        binder.bind(FilePathMaker.class).to(IDFilePathMaker.class).in(Scopes.SINGLETON);

        binder.bind(FileObjectSyncProcessor.class).to(DefaultFileObjectSyncProcessor.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectSynchronizer.class).to(DefaultFileObjectSynchronier.class);

        binder.bind(FileNodeStorer.class).to(ZkFileNodeStorer.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectFactory.class).to(DefaultFileObjectFactory.class).in(Scopes.SINGLETON);
        binder.bind(FileObjectCloser.class).to(DefaultFileObjectCloser.class);
        binder.bind(FileNodeSinkManager.class).to(ZkFileNodeSinkManager.class);
        binder.bind(FileNodeSinkSelector.class).toInstance(new RandomFileNodeSinkSelector());
        binder.bind(FileObjectSupplierFactory.class).to(DefaultFileObjectSupplierFactory.class).in(Scopes.SINGLETON);

        binder.bind(DataPoolFactory.class).to(BlockingQueueDataPoolFactory.class).in(Scopes.SINGLETON);
        binder.bind(DiskWriter.class).in(ManageLifecycle.class);
        binder.bind(DataEngineFactory.class).to(DefaultDataEngineFactory.class).in(Scopes.SINGLETON);
        binder.bind(DataEngineManager.class).to(DefaultDataEngineManager.class);


        binder.bind(StorageRegionWriter.class).to(DefaultStorageRegionWriter.class).in(Scopes.SINGLETON);
        binder.bind(RocksDBManager.class).to(NonRocksDBManager.class).in(Scopes.SINGLETON);
        binder.bind(BrfsCatalog.class).to(DefaultBrfsCatalog.class).in(Scopes.SINGLETON);

        jaxrs(binder).resource(DiscoveryResource.class);
        jaxrs(binder).resource(RouterResource.class);

        jaxrs(binder).resource(JsonMapper.class);

        jaxrs(binder).resource(DataResource.class);
        jaxrs(binder).resource(FSPackageProtoMapper.class);

        LifecycleModule.register(binder, SimpleAuthentication.class);
        LifecycleModule.register(binder, MetadataBackupServer.class);

        binder.requestStaticInjection(CuratorCacheFactory.class);

        binder.bind(Deliver.class).toInstance(Deliver.NOOP);
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
    public Service getService(
            ClusterConfig clusterConfig,
            HttpServerConfig serverConfig,
            ServiceManager serviceManager,
            Lifecycle lifecycle) {
        String host = serverConfig.getHost();
        if (host == null) {
            List<InetAddress> addresses = NetworkUtils.getAllLocalIps();
            if (addresses.isEmpty()) {
                throw new RuntimeException("no network interface is found");
            }

            host = addresses.get(0).getHostAddress();
        }

        Service service = new Service(
                UUID.randomUUID().toString(),
                clusterConfig.getRegionNodeGroup(),
                host,
                serverConfig.getPort());

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
    public BlockManager getBlockManager(
            StorageRegionWriter writer,
            BlockPool blockpool,
            StorageRegionWriter write,
            BrfsCatalog brfsCatalog) {
        return new SeqBlockManagerV2(blockpool, writer, brfsCatalog);
    }

    @Provides
    @Singleton
    public BlockPool getBlockPool() {
        long blocksize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);
        int maxCount = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_CAPACITY);
        Integer initCount = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_INIT_COUNT);
        return new SeqBlockPool(blocksize, maxCount, initCount);
    }
}
