package com.bonree.brfs.client.route;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;

import com.bonree.brfs.client.meta.ServiceMetaListener;
import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.meta.impl.DuplicaServiceMetaCache;
import com.bonree.brfs.client.route.listener.RouteCacheListener;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class ServiceSelectorManager implements Closeable {

    private String zkServerIDPath;
    private String baseRoutePath;
    private ServiceManager serviceManager;
    private Map<Integer, DiskServiceSelectorCache> diskServiceSelectorCachaMap = new ConcurrentHashMap<>();
    private DuplicaServiceSelector duplicaServiceSelector = null;
    private CuratorClient curatorClient;
    private TreeCache treeCache;
    
    private final String duplicateServiceGroup;
    private final String diskServiceGroup;

    public ServiceSelectorManager(final CuratorFramework client, String nameSpace, final String zkServerIDPath,
    		final String baseRoutePath,
    		ServiceManager serviceManager,
    		String duplicateServiceGroup,
    		String diskServiceGroup) throws Exception {
        this.zkServerIDPath = zkServerIDPath;
        this.baseRoutePath = baseRoutePath;
        this.curatorClient = CuratorClient.wrapClient(client);
        treeCache = new TreeCache(client, baseRoutePath);
        
        this.serviceManager = serviceManager;
        
        this.duplicateServiceGroup = duplicateServiceGroup;
        this.diskServiceGroup = diskServiceGroup;
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

        DiskServiceMetaCache diskServiceMetaCache = new DiskServiceMetaCache(curatorClient, zkServerIDPath, snIndex,
        		diskServiceGroup);
        ServiceMetaListener diskListener = new ServiceMetaListener(diskServiceMetaCache);
        serviceManager.addServiceStateListener(diskServiceGroup, diskListener);
        
        diskServiceMetaCache.loadMetaCachae(serviceManager);

        RouteRoleCache routeCache = new RouteRoleCache(curatorClient, snIndex, baseRoutePath);
        RouteParser routeParser = new RouteParser(routeCache);

        // 兼容余鹏的client读取
        diskServiceSelectorCache = new DiskServiceSelectorCache(diskServiceMetaCache, routeParser);

        RouteCacheListener cacheListener = new RouteCacheListener(routeCache);
        treeCache.getListenable().addListener(cacheListener);

        diskServiceSelectorCachaMap.put(snIndex, diskServiceSelectorCache);
        return diskServiceSelectorCache;
    }

    public DuplicaServiceSelector useDuplicaSelector() throws Exception {
        if (duplicaServiceSelector != null) {
            return duplicaServiceSelector;
        }

        DuplicaServiceMetaCache duplicaServiceMetaCache = new DuplicaServiceMetaCache(duplicateServiceGroup);

        // 监听duplicaServiceCachecache
        ServiceMetaListener listener = new ServiceMetaListener(duplicaServiceMetaCache);
        serviceManager.addServiceStateListener(duplicateServiceGroup, listener);

        duplicaServiceMetaCache.loadMetaCachae(serviceManager);

        duplicaServiceSelector = new DuplicaServiceSelector(duplicaServiceMetaCache);
        return duplicaServiceSelector;
    }

    @Override
    public void close() {
        if (treeCache != null) {
            treeCache.close();
        }
        
        if(serviceManager != null) {
        	try {
				serviceManager.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
    }

}
