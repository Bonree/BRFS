package com.bonree.brfs.client.meta.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;

public class DuplicaServiceMetaCache implements ServiceMetaCache {

    private Map<String, Service> duplicaServerCache;
    private String group;

    public DuplicaServiceMetaCache(ServiceManager sm, String group) {
        duplicaServerCache = new ConcurrentHashMap<>();
        this.group = group;
        loadMetaCachae(sm);
    }

    private void loadMetaCachae(ServiceManager sm) {
        // load 副本管理
        List<Service> dupliServices = sm.getServiceListByGroup(group);
        for (Service service : dupliServices) {
            duplicaServerCache.put(service.getServiceId(), service);
        }

    }

    /** 概述：加载所有关于该SN的2级SID对应的1级SID
     * @param service
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void addService(Service service) {
        // serverID信息加载
        duplicaServerCache.put(service.getServiceId(), service);
    }

    /** 概述：移除该SN对应的2级SID对应的1级SID
     * @param service
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void removeService(Service service) {
        duplicaServerCache.remove(service.getServiceId());

    }

    @Override
    public Map<String, Service> getServerCache() {
        return duplicaServerCache;

    }
}
