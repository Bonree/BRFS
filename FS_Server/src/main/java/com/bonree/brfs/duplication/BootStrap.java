package com.bonree.brfs.duplication;

import java.io.Closeable;
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
import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.ProcessFinalizer;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;
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
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.ZkStorageIdBuilder;
import com.bonree.brfs.duplication.storagename.handler.CreateStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.DeleteStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.OpenStorageNameMessageHandler;
import com.bonree.brfs.duplication.storagename.handler.UpdateStorageNameMessageHandler;
import com.bonree.brfs.duplication.synchronize.DefaultFileSynchronier;
import com.bonree.brfs.duplication.synchronize.FileSynchronizer;
import com.bonree.brfs.server.identification.ServerIDManager;

public class BootStrap {
    private static final Logger LOG = LoggerFactory.getLogger(BootStrap.class);

    public static void main(String[] args) {
    	ProcessFinalizer finalizer = new ProcessFinalizer();
        
        try {
        	String brfsHome = System.getProperty("path");
            System.setProperty("name", "duplication");
            Configuration conf = Configuration.getInstance();
            conf.parse(brfsHome + "/config/server.properties");
            conf.initLogback(brfsHome + "/config/logback.xml");
            conf.printConfigDetail();
            LOG.info("Startup duplication server....");
            ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
            StorageConfig storageConfig = StorageConfig.parse(conf);

            CuratorCacheFactory.init(serverConfig.getZkHosts());
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
            ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
            finalizer.add(idManager);
            
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(serverConfig.getZkHosts(), 3000, 15000, retryPolicy);
            client.start();
            client.blockUntilConnected();
            
            finalizer.add(client);

            SimpleAuthentication simpleAuthentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client);
            UserModel model = simpleAuthentication.getUser("root");
            if (model == null) {
                LOG.error("please init server!!!");
                System.exit(1);
            }

            client = client.usingNamespace(zookeeperPaths.getBaseClusterName().substring(1));

            Service service = new Service(idManager.getFirstServerID(), ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, serverConfig.getHost(), serverConfig.getPort());
            ServiceManager serviceManager = new DefaultServiceManager(client);
            serviceManager.start();
            
            finalizer.add(serviceManager);

            StorageNameManager storageNameManager = new DefaultStorageNameManager(storageConfig, client, new ZkStorageIdBuilder(serverConfig.getZkHosts(), zookeeperPaths.getBaseSequencesPath()));
            storageNameManager.start();
            
            finalizer.add(storageNameManager);

            FileNodeStorer storer = new ZkFileNodeStorer(client, ZkFileCoordinatorPaths.COORDINATOR_FILESTORE);
            FileNodeSinkManager sinkManager = new ZkFileNodeSinkManager(client, serviceManager, storer, new RandomFileNodeServiceSelector());
            sinkManager.start();
            
            finalizer.add(sinkManager);

            FileCoordinator fileCoordinator = new FileCoordinator(client, storer, sinkManager);

            FilteredDiskNodeConnectionPool connectionPool = new FilteredDiskNodeConnectionPool();
            connectionPool.addFactory(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP, new VirtualDiskNodeConnectionPool());
            connectionPool.addFactory(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new HttpDiskNodeConnectionPool(serviceManager));
            finalizer.add(connectionPool);

            FileSynchronizer fileSynchronizer = new DefaultFileSynchronier(connectionPool, serviceManager, idManager);
            fileSynchronizer.start();
            
            finalizer.add(fileSynchronizer);

            DuplicationNodeSelector nodeSelector = new VirtualDuplicationNodeSelector(serviceManager, idManager);

            FileLimiterCloser fileLimiterCloser = new FileLimiterCloser(fileSynchronizer, connectionPool, fileCoordinator, idManager);

            FileLoungeFactory fileLoungeFactory = new DefaultFileLoungeFactory(service, fileCoordinator, nodeSelector, storageNameManager, idManager, connectionPool, fileSynchronizer);
            DuplicateWriter writer = new DuplicateWriter(service, fileLoungeFactory, fileCoordinator, fileSynchronizer, idManager, connectionPool, fileLimiterCloser, storageNameManager);
            
            HttpConfig config = new HttpConfig(serverConfig.getPort());
            config.setBacklog(1024);
            NettyHttpServer httpServer = new NettyHttpServer(config);
            httpServer.addHttpAuthenticator(new SimpleHttpAuthenticator(simpleAuthentication));

            NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
            requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(writer, storageNameManager));
            requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
            requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(serverConfig, zookeeperPaths, serviceManager, storageNameManager));
            httpServer.addContextHandler(DuplicationEnvironment.URI_DUPLICATION_NODE_ROOT, requestHandler);

            NettyHttpRequestHandler snRequestHandler = new NettyHttpRequestHandler();
            snRequestHandler.addMessageHandler("PUT", new CreateStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("POST", new UpdateStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("GET", new OpenStorageNameMessageHandler(storageNameManager));
            snRequestHandler.addMessageHandler("DELETE", new DeleteStorageNameMessageHandler(serverConfig, zookeeperPaths, storageNameManager, serviceManager));
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
        } catch (ConfigPathException e) {
            LOG.error("config file not exist!!!", e);
            System.exit(1);
        } catch (ConfigParseException e) {
            LOG.error("config file parse error!!!", e);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("launch server error!!!", e);
            System.exit(1);
        } finally {
        	Runtime.getRuntime().addShutdownHook(finalizer);
        }
    }

}
