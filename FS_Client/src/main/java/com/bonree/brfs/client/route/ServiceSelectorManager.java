package com.bonree.brfs.client.route;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;

import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.route.impl.ReaderServiceSelector;
import com.bonree.brfs.client.route.listener.RouteCacheListener;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class ServiceSelectorManager implements Closeable {

    private String zkServerIDPath;
    private String baseRoutePath;
    private ServiceManager serviceManager;
    private Map<Integer, ReaderServiceSelector> diskServiceSelectorCachaMap = new ConcurrentHashMap<>();
    private CuratorClient curatorClient;
    private TreeCache treeCache;
    
    private final String diskServiceGroup;

    public ServiceSelectorManager(final CuratorFramework client, String nameSpace, final String zkServerIDPath,
    		final String baseRoutePath,
    		ServiceManager serviceManager,
    		String diskServiceGroup) throws Exception {
        this.zkServerIDPath = zkServerIDPath;
        this.baseRoutePath = baseRoutePath;
        this.curatorClient = CuratorClient.wrapClient(client);
        treeCache = new TreeCache(client, baseRoutePath);
        
        this.serviceManager = serviceManager;
        
        this.diskServiceGroup = diskServiceGroup;
    }

    /** 概述：选择相应的selector缓存
     * @param snIndex
     * @return
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public ReaderServiceSelector useDiskSelector(int snIndex) throws Exception {
    	ReaderServiceSelector readServerSelector = diskServiceSelectorCachaMap.get(snIndex);
        if (readServerSelector != null) {
            return readServerSelector;
        }

        DiskServiceMetaCache diskServiceMetaCache = new DiskServiceMetaCache(curatorClient, zkServerIDPath, snIndex, diskServiceGroup);
        serviceManager.addServiceStateListener(diskServiceGroup, new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				diskServiceMetaCache.addService(service);
			}
			
			@Override
			public void serviceAdded(Service service) {
				diskServiceMetaCache.removeService(service);
			}
		});
        
        diskServiceMetaCache.loadMetaCachae(serviceManager);

        RouteRoleCache routeCache = new RouteRoleCache(curatorClient, snIndex, baseRoutePath);
        RouteParser routeParser = new RouteParser(routeCache);

        // 兼容余鹏的client读取
        readServerSelector = new ReaderServiceSelector(diskServiceMetaCache, routeParser);

        RouteCacheListener cacheListener = new RouteCacheListener(routeCache);
        treeCache.getListenable().addListener(cacheListener);

        diskServiceSelectorCachaMap.put(snIndex, readServerSelector);
        return readServerSelector;
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
