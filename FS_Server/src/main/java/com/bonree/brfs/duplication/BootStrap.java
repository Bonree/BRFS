package com.bonree.brfs.duplication;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HttpConfig;
import com.bonree.brfs.common.net.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.net.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.ProcessFinalizer;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
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
import com.bonree.brfs.duplication.datastream.file.FileLimiterStateRebuilder;
import com.bonree.brfs.duplication.datastream.file.FileLoungeFactory;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;
import com.bonree.brfs.duplication.storagename.StorageIdBuilder;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.handler.CreateStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.DeleteStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.OpenStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.UpdateStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.impl.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.impl.ZkStorageIdBuilder;
import com.bonree.brfs.duplication.synchronize.DefaultFileSynchronier;
import com.bonree.brfs.duplication.synchronize.FileSynchronizer;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.common.base.Charsets;
import com.google.common.io.CharSource;
import com.google.common.io.Files;

public class BootStrap {
    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
    	ProcessFinalizer finalizer = new ProcessFinalizer();
        
        try {
            System.setProperty("name", "duplication");
            
            String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
            
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddresses, 3000, 15000, retryPolicy);
            client.start();
            client.blockUntilConnected();
            
            finalizer.add(client);

            CuratorCacheFactory.init(zkAddresses);
            
            String clusterName = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(clusterName, zkAddresses);
            ServerIDManager idManager = new ServerIDManager(client, zookeeperPaths);

            SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client);
            UserModel model = simpleAuthentication.getUser("root");
            if (model == null) {
                LOG.error("please init server!!!");
                System.exit(1);
            }

            String host = Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_HOST);
    		int port = Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_PORT);
    		
    		CharSource idSource = Files.asCharSource(new File(System.getProperty(SystemProperties.PROP_SERVER_ID_DIR), "duplicatenode_id"), Charsets.UTF_8);
    		Service service = new Service(idSource.readFirstLine(),
            		Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_SERVICE_GROUP_NAME),
            		host, port);
            ServiceManager serviceManager = new DefaultServiceManager(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
            serviceManager.start();
            
            finalizer.add(serviceManager);

            StorageIdBuilder storageIdBuilder = new ZkStorageIdBuilder(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
            StorageNameManager storageNameManager = new DefaultStorageNameManager(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), storageIdBuilder);
            storageNameManager.start();
            
            finalizer.add(storageNameManager);

            FileNodeStorer storer = new ZkFileNodeStorer(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), ZkFileCoordinatorPaths.COORDINATOR_FILESTORE);
            FileNodeSinkManager sinkManager = new ZkFileNodeSinkManager(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), serviceManager, storer, new RandomFileNodeServiceSelector());
            sinkManager.start();
            
            finalizer.add(sinkManager);

            FileCoordinator fileCoordinator = new FileCoordinator(client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), storer, sinkManager);

            FilteredDiskNodeConnectionPool connectionPool = new FilteredDiskNodeConnectionPool();
            connectionPool.addFactory(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP, new VirtualDiskNodeConnectionPool());
            connectionPool.addFactory(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME), new HttpDiskNodeConnectionPool(serviceManager));
            finalizer.add(connectionPool);

            FileSynchronizer fileSynchronizer = new DefaultFileSynchronier(connectionPool, serviceManager, idManager);
            fileSynchronizer.start();
            
            finalizer.add(fileSynchronizer);

            DuplicationNodeSelector nodeSelector = new VirtualDuplicationNodeSelector(serviceManager, idManager);

            FileLimiterCloser fileLimiterCloser = new FileLimiterCloser(fileSynchronizer, connectionPool, fileCoordinator, idManager);
            
            FileLimiterStateRebuilder fileRebuilder = new FileLimiterStateRebuilder(connectionPool, idManager);

            FileLoungeFactory fileLoungeFactory = new DefaultFileLoungeFactory(service, fileCoordinator, nodeSelector, storageNameManager, idManager, connectionPool, fileSynchronizer, fileRebuilder);
            DuplicateWriter writer = new DuplicateWriter(service, fileLoungeFactory, fileCoordinator, fileSynchronizer, idManager, connectionPool, fileLimiterCloser, storageNameManager, fileRebuilder);
            
            HttpConfig config = new HttpConfig(port);
            config.setBacklog(1024);
            NettyHttpServer httpServer = new NettyHttpServer(config);
            httpServer.addHttpAuthenticator(new SimpleHttpAuthenticator(simpleAuthentication));

            NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
            requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(writer, storageNameManager));
            requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
            requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(zookeeperPaths, serviceManager, storageNameManager));
            httpServer.addContextHandler(DuplicationEnvironment.URI_DUPLICATION_NODE_ROOT, requestHandler);

            NettyHttpRequestHandler snRequestHandler = new NettyHttpRequestHandler();
            snRequestHandler.addMessageHandler("PUT", new CreateStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("POST", new UpdateStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("GET", new OpenStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("DELETE", new DeleteStorageNameMessageHandler(zookeeperPaths, storageNameManager, serviceManager));
            httpServer.addContextHandler(DuplicationEnvironment.URI_STORAGENAME_NODE_ROOT, snRequestHandler);

            httpServer.start();
            
            finalizer.add(httpServer);

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
        } catch (Exception e) {
            LOG.error("launch server error!!!", e);
            System.exit(1);
        } finally {
        	Runtime.getRuntime().addShutdownHook(finalizer);
        }
    }

}
