package com.bonree.brfs.rebalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;
    private TaskOperation opt = null;
    ServiceManager serviceManager;

    public RebalanceManager(String zkHosts, String dataDir, ZookeeperPaths zkPaths, ServerIDManager idManager, ServiceManager serviceManager) {
        CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts, 500, 500);
        this.serviceManager = serviceManager;
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager, serviceManager);
        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), idManager, new DefaultStorageNameManager(curatorClient.getInnerClient()), dataDir);

    }

    public void start() throws Exception {
        LOG.info("start taskdispatch service!!");
        dispatch.start();
        LOG.info("start taskoperation service!!");
        opt.start();
    }

}
