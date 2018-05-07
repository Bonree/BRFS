package com.bonree.brfs.client.route;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.meta.ServiceMetaListener;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.ServiceManager;

public class ServiceSelectorManager {

    private String zkHosts;
    private String zkServerIDPath;
    private String baseRoutePath;
    private ServiceManager sm;
    private Map<Integer, ServiceSelectorCache> serviceSelectorCachaMap = new ConcurrentHashMap<>();

    private ServiceSelectorManager(final ServiceManager sm, final String zkHosts, final String zkServerIDPath, final String baseRoutePath) {
        this.zkHosts = zkHosts;
        this.zkServerIDPath = zkServerIDPath;
        this.baseRoutePath = baseRoutePath;
        this.sm = sm;
    }

    /** 概述：选择相应的selector缓存
     * @param snIndex
     * @return
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    ServiceSelectorCache useStorageIndex(int snIndex) throws Exception {
        ServiceSelectorCache serviceSelectorCache = serviceSelectorCachaMap.get(snIndex);
        if (serviceSelectorCache != null) {
            return serviceSelectorCache;
        }
        ServiceMetaCache serviceMetaCache = new ServiceMetaCache(zkHosts, zkServerIDPath, snIndex);
        ServiceMetaListener listener = new ServiceMetaListener(serviceMetaCache);
        sm.addServiceStateListener(Constants.DISCOVER, listener);
        RouteParser routeParser = new RouteParser(zkHosts, snIndex, baseRoutePath);
        serviceSelectorCache = new ServiceSelectorCache(serviceMetaCache, routeParser);
        serviceSelectorCachaMap.put(snIndex, serviceSelectorCache);
        return serviceSelectorCache;
    }

}
