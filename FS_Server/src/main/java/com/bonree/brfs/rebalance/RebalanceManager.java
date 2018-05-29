package com.bonree.brfs.rebalance;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.rebalance.transfer.SimpleFileServer;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;
    private TaskOperation opt = null;
    StorageNameManager snManager;
    SimpleFileServer fileServer = null;
    ExecutorService simpleFileServer = Executors.newSingleThreadExecutor();
    private CuratorClient curatorClient = null;

    public RebalanceManager(ServerConfig serverConfig, ZookeeperPaths zkPaths, ServerIDManager idManager, StorageNameManager snManager, ServiceManager serviceManager) {
        curatorClient = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 500, 500);
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager, serviceManager, snManager, serverConfig.getVirtualDelay(), serverConfig.getNormalDelay());
        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager, serverConfig.getDataPath(), snManager, serviceManager);
        try {
            fileServer = new SimpleFileServer(serverConfig.getDiskPort() + 20, serverConfig.getDataPath(), 10);
        } catch (IOException e) {
            LOG.info("fileServer launch error!!!", e);
        }
    }

    public void start() throws Exception {
        LOG.info("start taskdispatch service!!");
        dispatch.start();
        LOG.info("start taskoperation service!!");
        opt.start();
        LOG.info("start Simple file server!!!");
        simpleFileServer.execute(new Runnable() {
            @Override
            public void run() {
                fileServer.start();
            }
        });

    }

    @Override
    public void close() throws IOException {

        if (dispatch != null) {
        }

        if (opt != null) {

        }

        if (fileServer != null) {
            fileServer.close();
        }
        if (curatorClient != null) {
            curatorClient.close();
        }
    }

}
