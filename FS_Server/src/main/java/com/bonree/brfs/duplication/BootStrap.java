package com.bonree.brfs.duplication;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.configuration.StorageConfig;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.coordinator.zk.RandomFileNodeServiceSelector;
import com.bonree.brfs.duplication.coordinator.zk.ZkFileCoordinatorPaths;
import com.bonree.brfs.duplication.coordinator.zk.ZkFileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.zk.ZkFileNodeStorer;
import com.bonree.brfs.duplication.datastream.DuplicateWriter;
import com.bonree.brfs.duplication.datastream.connection.FilteredDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.http.HttpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.virtual.VirtualDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.DefaultFileLoungeFactory;
import com.bonree.brfs.duplication.datastream.file.FileLimiterCloser;
import com.bonree.brfs.duplication.datastream.file.FileLoungeFactory;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;
import com.bonree.brfs.duplication.recovery.DefaultFileSynchronier;
import com.bonree.brfs.duplication.recovery.FileSynchronizer;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.ZkStorageIdBuilder;
import com.bonree.brfs.duplication.storagename.handler.CreateStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.DeleteStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.OpenStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.UpdateStorageNameMessageHandler;
import com.bonree.brfs.server.identification.ServerIDManager;

public class BootStrap {
	private static final Logger LOG = LoggerFactory.getLogger("Main");

	public static void main(String[] args) throws Exception {
		String brfsHome = System.getProperty("brfs_home");
		
		Configuration conf = Configuration.getInstance();
        conf.parse(brfsHome + "/config/server.properties");
        conf.printConfigDetail();
        ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
        StorageConfig storageConfig = StorageConfig.parse(conf);

        CuratorCacheFactory.init(serverConfig.getZkHosts());
        ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
        ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(serverConfig.getZkHosts(), 3000, 15000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client);
		
		client = client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1));
		
		Service service = new Service(idManager.getFirstServerID(), ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, serverConfig.getHost(), serverConfig.getPort());
		ServiceManager serviceManager = new DefaultServiceManager(client);
		serviceManager.start();
		serviceManager.registerService(service);
		serviceManager.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				LOG.info("service Removed[{}]", service.getServiceId());
			}
			
			@Override
			public void serviceAdded(Service service) {
				LOG.info("service Added[{}]", service.getServiceId());
			}
			
		});
		
		StorageNameManager storageNameManager = new DefaultStorageNameManager(storageConfig,client, new ZkStorageIdBuilder(serverConfig.getZkHosts(), zookeeperPaths.getBaseSequencesPath()));
		storageNameManager.start();
		
		FileNodeStorer storer = new ZkFileNodeStorer(client, ZkFileCoordinatorPaths.COORDINATOR_FILESTORE);
		FileNodeSinkManager sinkManager = new ZkFileNodeSinkManager(client, serviceManager, storer, new RandomFileNodeServiceSelector());
		sinkManager.start();
		
		FileCoordinator fileCoordinator = new FileCoordinator(client, storer, sinkManager);
		
		FilteredDiskNodeConnectionPool connectionPool = new FilteredDiskNodeConnectionPool();
		connectionPool.addFactory(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP, new VirtualDiskNodeConnectionPool());
		connectionPool.addFactory(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new HttpDiskNodeConnectionPool(serviceManager));
		
		FileNodeStorer recoveryStorer = new ZkFileNodeStorer(client, ZkFileCoordinatorPaths.COORDINATOR_RECOVERY);
		FileSynchronizer fileRecovery = new DefaultFileSynchronier(connectionPool, recoveryStorer, idManager);
		fileRecovery.start();
		
		DuplicationNodeSelector nodeSelector = new VirtualDuplicationNodeSelector(serviceManager, idManager);
		
		FileLimiterCloser fileLimiterCloser = new FileLimiterCloser(fileRecovery, connectionPool, fileCoordinator, serviceManager, idManager);
		
		FileLoungeFactory fileLoungeFactory = new DefaultFileLoungeFactory(service, fileCoordinator, nodeSelector, storageNameManager, idManager, connectionPool);
		DuplicateWriter writer = new DuplicateWriter(service, fileLoungeFactory, fileCoordinator, fileRecovery, idManager, connectionPool, fileLimiterCloser);
		
		HttpConfig config = new HttpConfig(serverConfig.getPort());
		config.setKeepAlive(true);
		NettyHttpServer httpServer = new NettyHttpServer(config);
		httpServer.addHttpAuthenticator(new SimpleHttpAuthenticator(simpleAuthentication));
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(writer,storageNameManager));
		requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
		requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(serverConfig,zookeeperPaths,serviceManager, storageNameManager));
		NettyHttpContextHandler contextHttpHandler = new NettyHttpContextHandler(DuplicationEnvironment.URI_DUPLICATION_NODE_ROOT, requestHandler);
		httpServer.addContextHandler(contextHttpHandler);
		
		NettyHttpRequestHandler snRequestHandler = new NettyHttpRequestHandler();
		snRequestHandler.addMessageHandler("PUT", new CreateStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("POST", new UpdateStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("GET", new OpenStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("DELETE", new DeleteStorageNameMessageHandler(serverConfig,zookeeperPaths,storageNameManager,serviceManager));
		NettyHttpContextHandler snContextHandler = new NettyHttpContextHandler(DuplicationEnvironment.URI_STORAGENAME_NODE_ROOT, snRequestHandler);
		httpServer.addContextHandler(snContextHandler);
		
		httpServer.start();
	}

}
