package com.bonree.brfs.rebalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.rebalance.task.listener.ServerChangeListener;
import com.bonree.brfs.rebalance.task.listener.TaskExecutorListener;
import com.bonree.brfs.rebalance.task.listener.TaskStatusListener;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;

    private TaskOperation opt = null;

    private final String zkHosts;
    private final String baseRebalancePath;
    private final String baseRoutesPath;
    private final ServerIDManager idManager;
    
    CuratorTreeCache treeCache;

    public RebalanceManager(String zkHosts, String baseRebalancePath, String baseRoutesPath, ServerIDManager idManager) {
        this.zkHosts = zkHosts;
        this.baseRebalancePath = baseRebalancePath;
        this.baseRoutesPath = baseRoutesPath;
        this.idManager = idManager;
        treeCache = CuratorCacheFactory.getTreeCache();

    }

    public void start() throws Exception {
        CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts, 500, 500);
        dispatch = new TaskDispatcher(curatorClient, baseRebalancePath, baseRoutesPath, idManager);
        opt = new TaskOperation(curatorClient, baseRebalancePath, idManager);
        
        String changeMonitorPath = baseRebalancePath + Constants.SEPARATOR + Constants.CHANGES_NODE;
        LOG.info("changeMonitorPath:" + changeMonitorPath);
        treeCache.addListener(changeMonitorPath, new ServerChangeListener("server_change",dispatch));
        treeCache.startCache(changeMonitorPath);

        String tasksPath = baseRebalancePath + Constants.SEPARATOR + Constants.TASKS_NODE;
        LOG.info("tasksPath:" + tasksPath);
        treeCache.addListener(tasksPath, new TaskStatusListener("task_status",dispatch));
        treeCache.addListener(tasksPath, new TaskExecutorListener("task_executor", opt));
        treeCache.startCache(tasksPath);
        
    }

}
