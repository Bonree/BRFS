package com.bonree.brfs.rebalanceV2;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.RebalanceConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.rebalanceV2.task.TaskDispatcherV2;
import com.bonree.brfs.rebalanceV2.task.TaskOperationV2;
import com.bonree.brfs.rebalanceV2.transfer.SimpleFileServer;
import com.bonree.brfs.server.identification.ServerIDManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RebalanceManagerV2 implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(RebalanceManagerV2.class);

    private TaskDispatcherV2 dispatch = null;
    private TaskOperationV2 opt = null;
    SimpleFileServer fileServer = null;
    ExecutorService simpleFileServer = Executors.newSingleThreadExecutor();
    private CuratorClient curatorClient = null;
    private LocalPartitionInterface partitionInterface;

    @Inject
    public RebalanceManagerV2(ZookeeperPaths zkPaths, ServerIDManager idManager, StorageRegionManager snManager, ServiceManager serviceManager, LocalPartitionInterface partitionInterface) {
    	this.partitionInterface = partitionInterface;
        String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        curatorClient = CuratorClient.getClientInstance(zkAddresses, 500, 500);
        dispatch = new TaskDispatcherV2(curatorClient, zkPaths.getBaseRebalancePath(),
        		zkPaths.getBaseRoutePath(), idManager,
        		serviceManager, snManager,
        		Configs.getConfiguration().GetConfig(RebalanceConfigs.CONFIG_VIRTUAL_DELAY),
        		Configs.getConfiguration().GetConfig(RebalanceConfigs.CONFIG_NORMAL_DELAY));
        
//        String dataPath = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT);
        opt = new TaskOperationV2(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager,
        		 snManager, serviceManager, partitionInterface);
        
		int port = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT);
        try {
            fileServer = new SimpleFileServer(port + 20, this.partitionInterface, 10);
        } catch (IOException e) {
            LOG.info("fileServer launch error!!!", e);
        }
    }

    @LifecycleStart
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

    @LifecycleStop
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
