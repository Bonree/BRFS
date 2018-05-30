package com.bonree.brfs.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.configuration.StorageConfig;
import com.bonree.brfs.disknode.boot.EmptyMain;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.bonree.brfs.rebalance.RebalanceManager;
import com.bonree.brfs.rebalance.task.ServerChangeTaskGenetor;
import com.bonree.brfs.schedulers.InitTaskManager;
import com.bonree.brfs.server.identification.ServerIDManager;

public class ServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);
    static {
        // 加载 logback配置信息
        try {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(Configuration.class.getResourceAsStream("/logback.xml"));
            StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            help();
            System.exit(1);
        }
        String brfsHome = args[0];
        try {
            Configuration conf = Configuration.getInstance();
            conf.parse(brfsHome + "/config/server.properties");
            conf.printConfigDetail();
            ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
            StorageConfig storageConfig = StorageConfig.parse(conf);
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
            
            ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
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
                }
            });
            snManager.start();

            ServiceManager sm = new DefaultServiceManager(client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
            sm.start();

            Service selfService = new Service();
            selfService.setHost(serverConfig.getHost());
            selfService.setPort(serverConfig.getDiskPort());
            selfService.setServiceGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
            selfService.setServiceId(idManager.getFirstServerID());

            sm.registerService(selfService);
            System.out.println(selfService);

            // 磁盘管理模块
            EmptyMain diskMain = new EmptyMain(serverConfig, sm);
            diskMain.start();

            // 副本平衡模块
            sm.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new ServerChangeTaskGenetor(leaderClient, client, sm, idManager, zookeeperPaths.getBaseRebalancePath(), 3000, snManager));
           
            @SuppressWarnings("resource")
            RebalanceManager rebalanceServer = new RebalanceManager(serverConfig, zookeeperPaths, idManager, snManager, sm);
            rebalanceServer.start();

            // 资源管理模块
            InitTaskManager.initManager(serverConfig, resourceConfig, zookeeperPaths, sm, snManager, idManager);
        } catch (ConfigPathException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ConfigParseException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void help() {
        LOG.error("parameter is error!");
    }

}
