package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.zookeeper.curator.cache.CuratorPathCache;
import com.bonree.brfs.server.model.StorageCacheModel;
import com.bonree.brfs.server.model.StorageModel;

public class ServersMonitor {
    
    private StorageCacheModel snCache;
    
    

//    public static void main(String[] args) throws InterruptedException {
//        String zkUrl = "192.168.101.86:2181";
//        String serversPath = "/brfs/wz/servers";
//        CuratorPathCache cachePath = CuratorPathCache.getPathCacheInstance(zkUrl);
//        ServersChangeListener listener = new ServersChangeListener("taskListener");
//        cachePath.addListener(serversPath, listener);
//        cachePath.startPathCache(serversPath);
//        Thread.sleep(Long.MAX_VALUE);
//    }

}
