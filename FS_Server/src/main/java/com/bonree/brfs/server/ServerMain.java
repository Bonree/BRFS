package com.bonree.brfs.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.Configuration.ConfigException;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.rebalance.RebalanceManager;
import com.bonree.brfs.rebalance.task.ServerChangeTaskGenetor;
import com.bonree.brfs.schedulers.InitTaskManager;
import com.bonree.brfs.server.identification.ServerIDManager;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

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
            ResourceTaskConfig resourceConfig = ResourceTaskConfig.parse(conf);

            CuratorCacheFactory.init(serverConfig.getZkHosts());
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
            ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
            idManager.getFirstServerID();

            StorageNameManager snManager = null;

            CuratorClient leaderClient = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 1000, 1000);
            CuratorClient client = CuratorClient.getClientInstance(serverConfig.getZkHosts());
            StorageNameManager snManage = new DefaultStorageNameManager(client.getInnerClient());
            ServiceManager sm = new DefaultServiceManager(client.getInnerClient().usingNamespace(zookeeperPaths.getBaseServersPath().substring(1, zookeeperPaths.getBaseServersPath().length())));
            sm.start();

            // 副本平衡模块
            RebalanceManager rebalanceServer = new RebalanceManager(serverConfig.getZkHosts(), serverConfig.getDataPath(), zookeeperPaths, idManager, snManage, sm);
            rebalanceServer.start();

            // 资源管理模块
            InitTaskManager.initManager(serverConfig, resourceConfig, zookeeperPaths, sm, snManage, idManager);

            Service selfService = new Service();
            selfService.setHost(serverConfig.getHost());
            selfService.setPort(serverConfig.getPort());
            selfService.setServiceGroup(DiskContext.DEFAULT_DISK_NODE_SERVICE_GROUP);
            selfService.setServiceId(idManager.getFirstServerID());
            sm.registerService(selfService);
            System.out.println(selfService);
            sm.addServiceStateListener(DiskContext.DEFAULT_DISK_NODE_SERVICE_GROUP, new ServerChangeTaskGenetor(leaderClient, client, sm, idManager, zookeeperPaths.getBaseRebalancePath(), 3000, snManager));
            System.out.println("launch Server 1");
        } catch (ConfigException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void help() {
        LOG.error("parameter is error!");
    }

}
