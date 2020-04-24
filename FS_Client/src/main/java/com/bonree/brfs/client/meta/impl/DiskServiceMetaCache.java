package com.bonree.brfs.client.meta.impl;

import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskServiceMetaCache {

    private static final Logger LOG = LoggerFactory.getLogger(DiskServiceMetaCache.class);

    private String zkServerIDPath;

    private int snIndex;

    private String group;

    private CuratorFramework zkClient;

    private Map<String, Service> firstServerCache;

    private Map<String, String> secondServerCache;

    public DiskServiceMetaCache(CuratorFramework curatorClient, String zkServerIDPath, int snIndex, String group) {
        firstServerCache = new ConcurrentHashMap<>();
        secondServerCache = new ConcurrentHashMap<>();
        this.zkServerIDPath = zkServerIDPath;
        this.snIndex = snIndex;
        this.group = group;
        this.zkClient = curatorClient;
    }

    private void loadSecondServerId(String serviceId) {
        try {
            byte[] data = zkClient.getData().forPath(ZKPaths.makePath(zkServerIDPath, serviceId, String.valueOf(snIndex)));
            if (data != null) {
                secondServerCache.put(new String(data, "utf-8"), serviceId);
            }
        } catch (Exception e) {
            LOG.warn("load server id error", e);
        }
    }

    /**
     * 概述：加载所有关于该SN的2级SID对应的1级SID
     *
     * @param service
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void loadMetaCachae(ServiceManager sm) {
        // load 1级serverid
        for (Service service : sm.getServiceListByGroup(group)) {
            firstServerCache.put(service.getServiceId(), service);
            // load 2级serverid
            loadSecondServerId(service.getServiceId());
        }
    }

    /**
     * 概述：ADD 一个server
     *
     * @param service
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void addService(Service service) {
        // serverID信息加载
        LOG.info("addService");
        firstServerCache.put(service.getServiceId(), service);

        loadSecondServerId(service.getServiceId());
    }

    /**
     * 概述：移除该SN对应的2级SID对应的1级SID
     *
     * @param service
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void removeService(Service service) {
        firstServerCache.remove(service.getServiceId(), service);
        // 移除1级SID对应的2级SID
        for (Entry<String, String> entry : secondServerCache.entrySet()) {
            if (service.getServiceId().equals(entry.getValue())) {
                secondServerCache.remove(entry.getKey());
            }
        }

    }

    public ServiceMetaInfo getFirstServerCache(String secondID) {
        return new ServiceMetaInfo() {

            @Override
            public Service getFirstServer() {
                String firstServerID = secondServerCache.get(secondID);
                if (firstServerID == null) {
                    return null;
                }
                return firstServerCache.get(firstServerID);
            }

            @Override
            public int getReplicatPot() {
                return 1;
            }

        };
    }

    public ServiceMetaInfo getSecondServerCache(String secondServerId, int replicatPot) {
        return new ServiceMetaInfo() {

            @Override
            public Service getFirstServer() {
                String firstServerID = secondServerCache.get(secondServerId);
                if (firstServerID == null) {
                    return null;
                }
                return firstServerCache.get(firstServerID);
            }

            @Override
            public int getReplicatPot() {
                return replicatPot;
            }

        };
    }

    public List<String> listSecondID() {
        return new ArrayList<String>(secondServerCache.keySet());
    }

    public List<String> listFirstID() {
        return new ArrayList<String>(firstServerCache.keySet());
    }

    public Map<String, Service> getServerCache() {
        return firstServerCache;
    }
}
