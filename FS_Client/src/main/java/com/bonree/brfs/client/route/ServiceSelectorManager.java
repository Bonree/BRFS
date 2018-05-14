package com.bonree.brfs.client.route;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.recipes.cache.TreeCache;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.meta.ServiceMetaListener;
import com.bonree.brfs.client.route.listener.RouteCacheListener;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;

public class ServiceSelectorManager {

    private String zkHosts;
    private String zkServerIDPath;
    private String baseRoutePath;
    private ServiceManager sm;
    private Map<Integer, ServiceSelectorCache> serviceSelectorCachaMap = new ConcurrentHashMap<>();
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
    public ServiceSelectorCache useStorageIndex(int snIndex) throws Exception {
        ServiceSelectorCache serviceSelectorCache = serviceSelectorCachaMap.get(snIndex);
        if (serviceSelectorCache != null) {
            return serviceSelectorCache;
        }
        ServiceMetaCache serviceMetaCache = new ServiceMetaCache(zkHosts, zkServerIDPath, snIndex, sm);
        ServiceMetaListener listener = new ServiceMetaListener(serviceMetaCache);

        sm.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, listener);
        sm.addServiceStateListener(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, listener);

        RouteRoleCache routeCache = new RouteRoleCache(zkHosts, snIndex, baseRoutePath);
        RouteParser routeParser = new RouteParser(routeCache);
        // 兼容余鹏的client读取
        serviceSelectorCache = new ServiceSelectorCache(serviceMetaCache, routeParser);
        RouteCacheListener cacheListener = new RouteCacheListener(routeCache);

        treeCache.getListenable().addListener(cacheListener);
        serviceSelectorCachaMap.put(snIndex, serviceSelectorCache);
        return serviceSelectorCache;
    }

    public Service getRandomService() throws Exception {
        Service service = null;
        sm.addServiceStateListener(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, null);
        List<Service> services = sm.getServiceListByGroup(ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP);
        if (services != null && !services.isEmpty()) {
            Random random = new Random();
            service = services.get(random.nextInt(services.size()));
        }
        return service;
    }

    public void close() {
        if (treeCache != null) {
            treeCache.close();
        }
    }

}
