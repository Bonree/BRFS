package com.bonree.brfs.rebalancev2;

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
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalancev2.task.TaskDispatcherV2;
import com.bonree.brfs.rebalancev2.task.TaskOperationV2;
import com.bonree.brfs.rebalancev2.transfer.SimpleFileServer;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceManagerV2 implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManagerV2.class);

    private TaskDispatcherV2 dispatch;
    private TaskOperationV2 opt;
    SimpleFileServer fileServer = null;
    ExecutorService simpleFileServer = Executors.newSingleThreadExecutor();
    private CuratorClient curatorClient;

    @Inject
    public RebalanceManagerV2(ZookeeperPaths zkPaths, IDSManager idManager, StorageRegionManager snManager,
                              ServiceManager serviceManager, LocalPartitionInterface partitionInterface,
                              DiskPartitionInfoManager partitionInfoManager, RouteLoader routeLoader) {
        String zkAddresses = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        curatorClient = CuratorClient.getClientInstance(zkAddresses, 500, 500);
        dispatch = new TaskDispatcherV2(curatorClient, zkPaths.getBaseRebalancePath(),
                                        zkPaths.getBaseV2RoutePath(), idManager,
                                        serviceManager, snManager,
                                        Configs.getConfiguration().getConfig(RebalanceConfigs.CONFIG_VIRTUAL_DELAY),
                                        Configs.getConfiguration().getConfig(RebalanceConfigs.CONFIG_NORMAL_DELAY),
                                        partitionInfoManager);

        opt = new TaskOperationV2(curatorClient, zkPaths.getBaseRebalancePath(), zkPaths.getBaseRoutePath(), idManager,
                                  snManager, serviceManager, partitionInterface, routeLoader);

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
