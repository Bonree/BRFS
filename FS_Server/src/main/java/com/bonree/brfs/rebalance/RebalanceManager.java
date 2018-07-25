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
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.RebalanceConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.rebalance.transfer.SimpleFileServer;
import com.bonree.brfs.server.identification.ServerIDManager;

public class RebalanceManager implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch = null;
    private TaskOperation opt = null;
    SimpleFileServer fileServer = null;
    ExecutorService simpleFileServer = Executors.newSingleThreadExecutor();
    private CuratorClient curatorClient = null;

    public RebalanceManager(ZookeeperPaths zkPaths, ServerIDManager idManager, StorageRegionManager snManager, ServiceManager serviceManager) {
    	String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        curatorClient = CuratorClient.getClientInstance(zkAddresses, 500, 500);
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(),
        		zkPaths.getBaseRoutePath(), idManager,
        		serviceManager, snManager,
        		Configs.getConfiguration().GetConfig(RebalanceConfigs.CONFIG_VIRTUAL_DELAY),
        		Configs.getConfiguration().GetConfig(RebalanceConfigs.CONFIG_NORMAL_DELAY));
        
        String dataPath = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT);
        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager,
        		dataPath, snManager, serviceManager);
        
		int port = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT);
        try {
            fileServer = new SimpleFileServer(port + 20, dataPath, 10);
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
    	simpleFileServer.shutdown();

        if (dispatch != null) {
        	dispatch.close();
        }

        if (opt != null) {
        	opt.close();
        }

        if (fileServer != null) {
            fileServer.close();
        }
        if (curatorClient != null) {
            curatorClient.close();
        }
    }

}
