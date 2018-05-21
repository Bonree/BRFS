package com.bonree.brfs.rebalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;
    private TaskOperation opt = null;
    ServiceManager serviceManager;
    StorageNameManager snManager;

    public RebalanceManager(ServerConfig serverConfig, ZookeeperPaths zkPaths, ServerIDManager idManager, StorageNameManager snManager, ServiceManager serviceManager) {
        CuratorClient curatorClient = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 500, 500);
        this.serviceManager = serviceManager;
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager, serviceManager,snManager,serverConfig.getVirtualDelay(),serverConfig.getNormalDelay());
        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), idManager, serverConfig.getDataPath(), snManager, serviceManager);

    }

    public void start() throws Exception {
        LOG.info("start taskdispatch service!!");
        dispatch.start();
        LOG.info("start taskoperation service!!");
        opt.start();
    }

}
