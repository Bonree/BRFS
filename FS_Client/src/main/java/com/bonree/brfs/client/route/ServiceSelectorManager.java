package com.bonree.brfs.client.route;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.recipes.cache.TreeCache;

import com.bonree.brfs.client.meta.ServiceMetaListener;
import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.meta.impl.DuplicaServiceMetaCache;
import com.bonree.brfs.client.route.listener.RouteCacheListener;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;

public class ServiceSelectorManager {

    private String zkHosts;
    private String zkServerIDPath;
    private String baseRoutePath;
    private ServiceManager sm;
    private Map<Integer, DiskServiceSelectorCache> diskServiceSelectorCachaMap = new ConcurrentHashMap<>();
    private DuplicaServiceSelector duplicaServiceSelector = null;
    private TreeCache treeCache;

    public ServiceSelectorManager(final ServiceManager sm, final String zkHosts, final String zkServerIDPath, final String baseRoutePath) {
        this.zkHosts = zkHosts;
        this.zkServerIDPath = zkServerIDPath;
        this.baseRoutePath = baseRoutePath;
        this.sm = sm;
        CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts);
        treeCache = new TreeCache(curatorClient.getInnerClient(), baseRoutePath);
    }

    /** 概述：选择相应的selector缓存
     * @param snIndex
     * @return
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public DiskServiceSelectorCache useDiskSelector(int snIndex) throws Exception {
        DiskServiceSelectorCache diskServiceSelectorCache = diskServiceSelectorCachaMap.get(snIndex);
        if (diskServiceSelectorCache != null) {
            return diskServiceSelectorCache;
        }
        DiskServiceMetaCache diskServiceMetaCache = new DiskServiceMetaCache(zkHosts, zkServerIDPath, snIndex, ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, sm);

        ServiceMetaListener diskListener = new ServiceMetaListener(diskServiceMetaCache);
        sm.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, diskListener);

        RouteRoleCache routeCache = new RouteRoleCache(zkHosts, snIndex, baseRoutePath);
        RouteParser routeParser = new RouteParser(routeCache);

        // 兼容余鹏的client读取
        diskServiceSelectorCache = new DiskServiceSelectorCache(diskServiceMetaCache, routeParser);

        RouteCacheListener cacheListener = new RouteCacheListener(routeCache);
        treeCache.getListenable().addListener(cacheListener);

        diskServiceSelectorCachaMap.put(snIndex, diskServiceSelectorCache);
        return diskServiceSelectorCache;
    }

//    public Service getRandomService() throws Exception {
//        if (duplicaServiceSelector != null) {
//            return duplicaServiceSelector.randomService();
//        }
//
//        DuplicaServiceMetaCache duplicaServiceMetaCache = new DuplicaServiceMetaCache(sm, ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP);
//
//        // 监听duplicaServiceCachecache
//        ServiceMetaListener listener = new ServiceMetaListener(duplicaServiceMetaCache);
//        sm.addServiceStateListener(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, listener);
//
//        duplicaServiceSelector = new DuplicaServiceSelector(duplicaServiceMetaCache);
//        return duplicaServiceSelector.randomService();
//    }

    public DuplicaServiceSelector useDuplicaSelector() throws Exception {
        if (duplicaServiceSelector != null) {
            return duplicaServiceSelector;
        }

        DuplicaServiceMetaCache duplicaServiceMetaCache = new DuplicaServiceMetaCache(sm, ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP);

        // 监听duplicaServiceCachecache
        ServiceMetaListener listener = new ServiceMetaListener(duplicaServiceMetaCache);
        sm.addServiceStateListener(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, listener);

        duplicaServiceSelector = new DuplicaServiceSelector(duplicaServiceMetaCache);
        return duplicaServiceSelector;
    }

    public void close() {
        if (treeCache != null) {
            treeCache.close();
        }
    }

}
