package com.bonree.brfs.duplication;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.coordinator.zk.RandomFileNodeServiceSelector;
import com.bonree.brfs.duplication.coordinator.zk.ZkFileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.zk.ZkFileNodeStorer;
import com.bonree.brfs.duplication.datastream.DuplicateWriter;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionFactory;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.HttpDiskNodeConnectionFactory;
import com.bonree.brfs.duplication.datastream.connection.HttpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.DefaultFileLounge;
import com.bonree.brfs.duplication.datastream.file.FileLounge;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;
import com.bonree.brfs.duplication.recovery.DefaultFileRecovery;
import com.bonree.brfs.duplication.recovery.FileRecovery;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.handler.CreateStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.DeleteStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.OpenStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.UpdateStorageNameMessageHandler;

public class BootStrap {
	private static final Logger LOG = LoggerFactory.getLogger("Main");

	public static void main(String[] args) throws Exception {
		int port = 9001;
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.101.86:2181", 3000, 15000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		client = client.usingNamespace("brfstest");
		
		Service service = new Service("dup_1", "duplicate_group", "192.168.4.137", port);
		ServiceManager serviceManager = new DefaultServiceManager(client);
		serviceManager.start();
		serviceManager.registerService(service);
		serviceManager.addServiceStateListener("disk", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				LOG.info("service Removed[{}]", service.getServiceId());
			}
			
			@Override
			public void serviceAdded(Service service) {
				LOG.info("service Added[{}]", service.getServiceId());
			}
		});
		
		StorageNameManager storageNameManager = new DefaultStorageNameManager(client);
		storageNameManager.start();
		
		FileNodeStorer storer = new ZkFileNodeStorer(client);
		FileNodeSinkManager sinkManager = new ZkFileNodeSinkManager(client, serviceManager, storer, new RandomFileNodeServiceSelector());
		sinkManager.start();
		
		FileCoordinator fileCoordinator = new FileCoordinator(storer, sinkManager);
		
		DiskNodeConnectionFactory connectionFactory = new HttpDiskNodeConnectionFactory();
		DiskNodeConnectionPool connectionPool = new HttpDiskNodeConnectionPool(serviceManager, connectionFactory);
		FileRecovery fileRecovery = new DefaultFileRecovery(connectionPool);
		fileRecovery.start();
		
		DuplicationNodeSelector nodeSelector = new MockDuplicationNodeSelector();
		
		FileLounge lounge = new DefaultFileLounge(service, storageNameManager, fileCoordinator, fileRecovery, nodeSelector);
		DuplicateWriter writer = new DuplicateWriter(lounge, fileRecovery, connectionPool);
		
		HttpConfig config = new HttpConfig(port);
		NettyHttpServer httpServer = new NettyHttpServer(config);
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(writer));
		requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
		requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(serviceManager, storageNameManager));
		NettyHttpContextHandler contextHttpHandler = new NettyHttpContextHandler("/duplication", requestHandler);
		httpServer.addContextHandler(contextHttpHandler);
		
		NettyHttpRequestHandler snRequestHandler = new NettyHttpRequestHandler();
		snRequestHandler.addMessageHandler("PUT", new CreateStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("POST", new UpdateStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("GET", new OpenStorageNameMessageHandler(storageNameManager));
		snRequestHandler.addMessageHandler("DELETE", new DeleteStorageNameMessageHandler(storageNameManager));
		NettyHttpContextHandler snContextHandler = new NettyHttpContextHandler("/storageName", snRequestHandler);
		httpServer.addContextHandler(snContextHandler);
		
		httpServer.start();
	}

}
