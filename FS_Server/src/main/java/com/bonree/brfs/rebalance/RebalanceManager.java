package com.bonree.brfs.rebalance;

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
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.rebalance.transfer.SimpleFileServer;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    private TaskDispatcher dispatch;
    private TaskOperation opt;
    SimpleFileServer fileServer = null;
    ExecutorService simpleFileServer = Executors.newSingleThreadExecutor();
    private CuratorClient curatorClient;

    @Inject
    public RebalanceManager(ZookeeperPaths zkPaths, IDSManager idManager, StorageRegionManager snManager,
                            ServiceManager serviceManager, LocalPartitionInterface partitionInterface,
                            DiskPartitionInfoManager partitionInfoManager, RouteCache routeCache) {
        String zkAddresses = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        curatorClient = CuratorClient.getClientInstance(zkAddresses, 500, 500);
        dispatch = new TaskDispatcher(curatorClient, zkPaths.getBaseRebalancePath(),
                                      zkPaths.getBaseV2RoutePath(), idManager,
                                      serviceManager, snManager,
                                      Configs.getConfiguration().getConfig(RebalanceConfigs.CONFIG_VIRTUAL_DELAY),
                                      Configs.getConfiguration().getConfig(RebalanceConfigs.CONFIG_NORMAL_DELAY),
                                      partitionInfoManager);

        opt = new TaskOperation(curatorClient, zkPaths.getBaseRebalancePath(), idManager, snManager, serviceManager,
                                partitionInterface, routeCache);

        int port = Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_PORT);
        try {
            fileServer = new SimpleFileServer(port + 20, partitionInterface, 10);
        } catch (IOException e) {
            LOG.info("fileServer launch error!!!", e);
        }
    }

    @LifecycleStart
    public void start() throws Exception {
        LOG.info("start task dispatch service!!");
        dispatch.start();
        LOG.info("start task operation service!!");
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
