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
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
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
    	
        try {
            System.setProperty("name", "disk");
            ResourceTaskConfig resourceConfig = ResourceTaskConfig.parse();
            
            String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
            CuratorClient leaderClient = CuratorClient.getClientInstance(zkAddresses, 1000, 1000);
            CuratorClient client = CuratorClient.getClientInstance(zkAddresses);

            CuratorCacheFactory.init(zkAddresses);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME), zkAddresses);
            
            SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client.getInnerClient());
            UserModel model = authentication.getUser("root");
            if(model == null) {
                LOG.error("please init server!!!");
                System.exit(1);
            }
            
            ServerIDManager idManager = new ServerIDManager(client.getInnerClient(), zookeeperPaths);
            idManager.getFirstServerID();

            StorageNameManager snManager = new DefaultStorageNameManager(client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), null);
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
            EmptyMain diskMain = new EmptyMain(sm);
            diskMain.start();
            
            finalizer.add(diskMain);

            // 副本平衡模块
            sm.addServiceStateListener(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME),
            		new ServerChangeTaskGenetor(leaderClient, client, sm, idManager, zookeeperPaths.getBaseRebalancePath(), 3000, snManager));
           
            @SuppressWarnings("resource")
            RebalanceManager rebalanceServer = new RebalanceManager(zookeeperPaths, idManager, snManager, sm);
            rebalanceServer.start();
            
            String host = Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_HOST);
    		int port = Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_PORT);
            Service selfService = new Service();
            selfService.setHost(host);
            selfService.setPort(port);
            selfService.setServiceGroup(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME));
            String serviceId = idManager.getFirstServerID();
            selfService.setServiceId(serviceId);
            Service checkService = sm.getServiceById(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME), serviceId);
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
            InitTaskManager.initManager(resourceConfig, zookeeperPaths, sm, snManager, idManager);
        } catch (Exception e) {
            LOG.error("launch server error!!!",e);
            System.exit(1);
        } finally {
        	Runtime.getRuntime().addShutdownHook(finalizer);
        }
    }
}
