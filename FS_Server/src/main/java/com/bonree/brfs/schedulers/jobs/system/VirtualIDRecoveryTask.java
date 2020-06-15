package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.rebalance.route.RouteCache;
import org.apache.curator.framework.CuratorFramework;

public class VirtualIDRecoveryTask implements Runnable {
    private RouteCache routeCache;
    private CuratorFramework client;
    private StorageRegion region;
    private ServiceManager serviceManager;

    @Override
    public void run() {

    }
}
