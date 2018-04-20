package com.bonree.brfs.rebalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;
    private TaskOperation opt = null;

    public RebalanceManager(String zkHosts, ZookeeperPaths zkPaths, ServerIDManager idManager) {
        CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts, 500, 500);
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager);
        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), idManager);
    }

    public void start() throws Exception {
        LOG.info("start taskdispatch service!!");
        dispatch.start();
        LOG.info("start taskoperation service!!");
        opt.start();
    }

}
