package com.bonree.brfs.duplication;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HttpConfig;
import com.bonree.brfs.common.net.http.netty.HttpAuthenticator;
import com.bonree.brfs.common.net.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.net.http.netty.NettyHttpServer;
import com.bonree.brfs.common.net.tcp.client.AsyncTcpClientGroup;
import com.bonree.brfs.common.process.ProcessFinalizer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.IDFilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.tcp.TcpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.BlockingQueueDataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataPoolFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineFactory;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DefaultDataEngineManager;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectCloser;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectFactory;
import com.bonree.brfs.duplication.datastream.file.DefaultFileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectFactory;
import com.bonree.brfs.duplication.datastream.file.FileObjectSupplierFactory;
import com.bonree.brfs.duplication.datastream.file.sync.DefaultFileObjectSyncProcessor;
import com.bonree.brfs.duplication.datastream.file.sync.DefaultFileObjectSynchronier;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSyncProcessor;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.writer.DefaultStorageRegionWriter;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MachineResourceWriterSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MinimalDuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.ResourceWriteSelector;
import com.bonree.brfs.duplication.filenode.zk.RandomFileNodeSinkSelector;
import com.bonree.brfs.duplication.filenode.zk.ZkFileCoordinatorPaths;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeSinkManager;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeStorer;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.handler.CreateStorageRegionMessageHandler;
import com.bonree.brfs.duplication.storageregion.handler.DeleteStorageRegionMessageHandler;
import com.bonree.brfs.duplication.storageregion.handler.OpenStorageRegionMessageHandler;
import com.bonree.brfs.duplication.storageregion.handler.UpdateStorageRegionMessageHandler;
import com.bonree.brfs.duplication.storageregion.impl.DefaultStorageRegionManager;
import com.bonree.brfs.duplication.storageregion.impl.ZkStorageRegionIdBuilder;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.server.identification.ServerIDManager;

public class BootStrap {
    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    private static final String URI_DATA_ROOT = "/data";
    
    private static final String URI_STORAGE_REGION_ROOT = "/sr";

    private static final String URI_PING_ROOT = "/ping";

    private static final String URI_ROCKSDB_ROOT = "/rocksdb";

    public static void main(String[] args) {
    	ProcessFinalizer finalizer = new ProcessFinalizer();

        try {
            // 初始化email发送配置
            /** Module Managed **/
            EmailPool.getInstance();
            /********************/
            
            /** Module Managed **/
            String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
            String host = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_HOST);
    		int port = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_PORT);

            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddresses, 3000, 15000, retryPolicy);
            client.start();
            client.blockUntilConnected();

            finalizer.add(client);
            /********************/

            /** Module Managed **/
            CuratorCacheFactory.init(zkAddresses);
            /********************/

            /** Module Managed **/
            String clusterName = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(clusterName, client);
            ServerIDManager idManager = new ServerIDManager(client, zookeeperPaths);
            /********************/

            /** Module Managed **/
            SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseLocksPath(), client);
            simpleAuthentication.init(zookeeperPaths.getBaseUserPath());
            UserModel model = simpleAuthentication.getUser("root");
            if (model == null) {
                LOG.error("please init server!!!");
                System.exit(1);
            }
            /********************/

            /** Module Managed **/
            String regionGroupName = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME);
            String localServiceId = UUID.randomUUID().toString();
            Service service = new Service(localServiceId,
                    regionGroupName,
            		host, port);
            ServiceManager serviceManager = new DefaultServiceManager(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
            serviceManager.start();

            finalizer.add(serviceManager);
            /********************/

            /** Module Managed **/
            TimeExchangeEventEmitter timeEventEmitter = new TimeExchangeEventEmitter();

            finalizer.add(timeEventEmitter);
            /********************/

            /** Module Managed **/
            StorageRegionIdBuilder storageIdBuilder = new ZkStorageRegionIdBuilder(client, zookeeperPaths);
            StorageRegionManager storageNameManager = new DefaultStorageRegionManager(client, zookeeperPaths, storageIdBuilder);
            storageNameManager.start();

            finalizer.add(storageNameManager);
            /********************/

//            HttpDiskNodeConnectionPool connectionPool = new HttpDiskNodeConnectionPool(serviceManager);
//            finalizer.add(connectionPool);

            /** Module Managed **/
            AsyncTcpClientGroup tcpClientGroup = new AsyncTcpClientGroup(Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_WRITER_WORKER_NUM));
            TcpDiskNodeConnectionPool connectionPool = new TcpDiskNodeConnectionPool(serviceManager, tcpClientGroup);
            finalizer.add(tcpClientGroup);

            FilePathMaker pathMaker = new IDFilePathMaker(idManager);
            /********************/


            int workerThreadNum = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_SERVER_IO_THREAD_NUM);
            HttpConfig httpConfig = HttpConfig.newBuilder()
    				.setHost(host)
    				.setPort(port)
    				.setAcceptWorkerNum(1)
    				.setRequestHandleWorkerNum(workerThreadNum)
    				.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")))
    				.build();
            NettyHttpServer httpServer = new NettyHttpServer(httpConfig);
            httpServer.addHttpAuthenticator(new HttpAuthenticator() {

				@Override
				public int check(String userName, String passwd) {
					StringBuilder tokenBuilder = new StringBuilder();
					tokenBuilder.append(userName).append(":").append(passwd);

					return simpleAuthentication.auth(tokenBuilder.toString()) ? 0 : ReturnCode.USER_FORBID.getCode();
				}

            });

            ExecutorService requestHandlerExecutor = Executors.newFixedThreadPool(
            		Math.max(4, Runtime.getRuntime().availableProcessors() / 4),
    				new PooledThreadFactory("request_handler"));

            finalizer.add(new Closeable() {

				@Override
				public void close() throws IOException {
					requestHandlerExecutor.shutdown();
				}
			});

            /** Module Managed **/
            FileObjectSyncProcessor processor = new DefaultFileObjectSyncProcessor(connectionPool, pathMaker);
            DefaultFileObjectSynchronier fileSynchronizer = new DefaultFileObjectSynchronier(processor, serviceManager, 10, TimeUnit.SECONDS);
            fileSynchronizer.start();

            finalizer.add(fileSynchronizer);
            /********************/

            /** Module Managed **/
            FileNodeStorer storer = new ZkFileNodeStorer(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), ZkFileCoordinatorPaths.COORDINATOR_FILESTORE);
            /********************/
            
            /** Module Managed **/
            // 资源缓存器
            String diskGroup = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME);
            String rPath = zookeeperPaths.getBaseResourcesPath()+"/"+diskGroup+"/resource";
            ClusterResource clusterResource = ClusterResource.newBuilder()
                    .setCache(true)
                    .setClient(client)
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
            /********************/

            /** Module Managed **/
            FileObjectFactory fileFactory = new DefaultFileObjectFactory(service, storer, nodeSelector, idManager, connectionPool);
            /********************/

            /** Module Managed **/
            int closerThreadNum = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_CLOSER_THREAD_NUM);
            DefaultFileObjectCloser fileCloser = new DefaultFileObjectCloser(closerThreadNum, fileSynchronizer, storer, connectionPool, pathMaker);
            finalizer.add(fileCloser);
            /********************/

            /** Module Managed **/
            FileNodeSinkManager sinkManager = new ZkFileNodeSinkManager(
                    client,
                    zookeeperPaths,
            		service, serviceManager, timeEventEmitter, storer, new RandomFileNodeSinkSelector(), fileCloser);
            sinkManager.start();

            finalizer.add(sinkManager);
            /********************/

            /** Module Managed **/
            DataPoolFactory dataPoolFactory = new BlockingQueueDataPoolFactory(Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_DATA_POOL_CAPACITY));
            FileObjectSupplierFactory fileSupplierFactory = new DefaultFileObjectSupplierFactory(fileFactory,
            		fileCloser, fileSynchronizer, sinkManager, timeEventEmitter);

            DiskWriter diskWriter = new DiskWriter(Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_WRITER_WORKER_NUM),
            		connectionPool, pathMaker);
            finalizer.add(diskWriter);

            DataEngineFactory engineFactory = new DefaultDataEngineFactory(dataPoolFactory, fileSupplierFactory, diskWriter);
            DefaultDataEngineManager engineManager = new DefaultDataEngineManager(storageNameManager, engineFactory);

            finalizer.add(engineManager);
            /********************/

//            RegionNodeConnectionPool regionNodeConnectionPool = new HttpRegionNodeConnectionPool(serviceManager);

//            String rocksDBPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH);
//            RocksDBManager rocksDBManager = new DefaultRocksDBManager(rocksDBPath,
//                    client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)),
//                    localServiceId,
//                    serviceManager,
//                    regionNodeConnectionPool);
//            rocksDBManager.start();
//            finalizer.add(rocksDBManager);

            /** Module Managed **/
            DefaultStorageRegionWriter writer = new DefaultStorageRegionWriter(engineManager);
            /********************/

            NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
            requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(writer));
            requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
            requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(zookeeperPaths, serviceManager, storageNameManager));
            httpServer.addContextHandler(URI_DATA_ROOT, requestHandler);

            NettyHttpRequestHandler snRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
            snRequestHandler.addMessageHandler("PUT", new CreateStorageRegionMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("POST", new UpdateStorageRegionMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("GET", new OpenStorageRegionMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("DELETE", new DeleteStorageRegionMessageHandler(zookeeperPaths, storageNameManager, serviceManager));
            httpServer.addContextHandler(URI_STORAGE_REGION_ROOT, snRequestHandler);

//            NettyHttpRequestHandler pingRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
//            pingRequestHandler.addMessageHandler("GET", new PingRequestHandler());
//            httpServer.addContextHandler(URI_PING_ROOT, pingRequestHandler);
//
//            NettyHttpRequestHandler rocksDBRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
//            rocksDBRequestHandler.addMessageHandler("POST", new RocksDBWriteRequestHandler(rocksDBManager));
//            httpServer.addContextHandler(URI_ROCKSDB_ROOT, rocksDBRequestHandler);

            httpServer.start();

            finalizer.add(httpServer);

            /** Module Managed **/
            serviceManager.registerService(service);

            finalizer.add(new Closeable() {

				@Override
				public void close() throws IOException {
					try {
						serviceManager.unregisterService(service);
					} catch (Exception e) {
						LOG.error("unregister service[{}] error", service, e);
					}
				}
			});
            /********************/
        } catch (Exception e) {
            LOG.error("launch server error!!!", e);
            System.exit(1);
        } finally {
        	Runtime.getRuntime().addShutdownHook(finalizer);
        }
    }

}
