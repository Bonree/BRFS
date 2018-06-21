package com.bonree.brfs.server;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.ProcessFinalizer;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.configuration.StorageConfig;
import com.bonree.brfs.disknode.boot.EmptyMain;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.bonree.brfs.duplication.storagename.impl.DefaultStorageNameManager;
import com.bonree.brfs.rebalance.RebalanceManager;
import com.bonree.brfs.rebalance.task.ServerChangeTaskGenetor;
import com.bonree.brfs.schedulers.InitTaskManager;
import com.bonree.brfs.server.identification.ServerIDManager;

public class ServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);

    public static void main(String[] args) {
    	ProcessFinalizer finalizer = new ProcessFinalizer();
    	
        String brfsHome = System.getProperty("path");
        try {
            Configuration conf = Configuration.getInstance();
            System.setProperty("name", "disk");
            conf.parse(brfsHome + "/config/server.properties");
            conf.initLogback(brfsHome + "/config/logback.xml");
            conf.printConfigDetail();
            LOG.info("Startup disk server....");
            ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
            StorageConfig storageConfig = StorageConfig.parse(conf);
            LOG.info("serverConfig:"+serverConfig);
            LOG.info("storageConfig"+storageConfig);
            ResourceTaskConfig resourceConfig = ResourceTaskConfig.parse(conf);
            
            CuratorClient leaderClient = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 1000, 1000);
            CuratorClient client = CuratorClient.getClientInstance(serverConfig.getZkHosts());

            CuratorCacheFactory.init(serverConfig.getZkHosts());
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
            
            SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client.getInnerClient());
            UserModel model = authentication.getUser("root");
            if(model == null) {
                LOG.error("please init server!!!");
                System.exit(1);
            }
            
            ServerIDManager idManager = new ServerIDManager(client.getInnerClient(), serverConfig, zookeeperPaths);
            idManager.getFirstServerID();

            StorageNameManager snManager = new DefaultStorageNameManager(storageConfig, client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), null);
            snManager.addStorageNameStateListener(new StorageNameStateListener() {
                @Override
                public void storageNameAdded(StorageNameNode node) {
                    LOG.info("-----------StorageNameAdded--[{}]", node);
                    idManager.getSecondServerID(node.getId());
                }

                @Override
                public void storageNameUpdated(StorageNameNode node) {
                }

                @Override
                public void storageNameRemoved(StorageNameNode node) {
                    LOG.info("-----------StorageNameRemove--[{}]", node);
                    idManager.deleteSecondServerID(node.getId());
                }
            });
            snManager.start();
            
            finalizer.add(snManager);

            ServiceManager sm = new DefaultServiceManager(client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
            sm.start();
            
            finalizer.add(sm);

            // 磁盘管理模块
            EmptyMain diskMain = new EmptyMain(serverConfig, sm);
            diskMain.start();
            
            finalizer.add(diskMain);

            // 副本平衡模块
            sm.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new ServerChangeTaskGenetor(leaderClient, client, sm, idManager, zookeeperPaths.getBaseRebalancePath(), 3000, snManager));
           
            @SuppressWarnings("resource")
            RebalanceManager rebalanceServer = new RebalanceManager(serverConfig, zookeeperPaths, idManager, snManager, sm);
            rebalanceServer.start();
            
            Service selfService = new Service();
            selfService.setHost(serverConfig.getHost());
            selfService.setPort(serverConfig.getDiskPort());
            selfService.setServiceGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
            String serviceId = idManager.getFirstServerID();
            selfService.setServiceId(serviceId);
            Service checkService = sm.getServiceById(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, serviceId);
            if(checkService == null) {
            	sm.registerService(selfService);
            	System.out.println(selfService);
            }else {
            	LOG.error("serviceId : {} is exists, system will exit!!!",serviceId);
            	snManager.stop();
            	sm.stop();
            	System.exit(1);
            }
            
            finalizer.add(new Closeable() {
				
				@Override
				public void close() throws IOException {
					try {
						sm.unregisterService(selfService);
					} catch (Exception e) {
						LOG.error("unregister service[{}] error", selfService, e);
					}
				}
			});
            
         // 资源管理模块
            InitTaskManager.initManager(serverConfig, resourceConfig, zookeeperPaths, sm, snManager, idManager);
        } catch (ConfigPathException e) {
            LOG.error("config file not exist!!!",e);
            System.exit(1);
        } catch (ConfigParseException e) {
            LOG.error("config file parse error!!!",e);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("launch server error!!!",e);
            System.exit(1);
        } finally {
        	Runtime.getRuntime().addShutdownHook(finalizer);
        }
    }
}
