package com.bonree.brfs.client.meta.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class DiskServiceMetaCache implements ServiceMetaCache {

    private static final String SEPARATOR = "/";

    private String zkServerIDPath;

    private String zkHosts;

    private int snIndex;

    private String group;

    private Map<String, Service> firstServerCache;

    private Map<String, String> secondServerCache;

    public DiskServiceMetaCache(final String zkHosts, final String zkServerIDPath, final int snIndex, String group) {
        firstServerCache = new ConcurrentHashMap<>();
        secondServerCache = new ConcurrentHashMap<>();
        this.zkHosts = zkHosts;
        this.zkServerIDPath = zkServerIDPath;
        this.snIndex = snIndex;
        this.group = group;
    }

    public void loadMetaCachae(ServiceManager sm) {
        // 加载元数据信息
        CuratorClient curatorClient = null;
        try {
            curatorClient = CuratorClient.getClientInstance(zkHosts);
            List<Service> diskServices = sm.getServiceListByGroup(group);
            // load 1级serverid
            for (Service service : diskServices) {
                firstServerCache.put(service.getServiceId(), service);
                // load 2级serverid
                String snPath = zkServerIDPath + SEPARATOR + service.getServiceId() + SEPARATOR + snIndex;
                if (!curatorClient.checkExists(snPath)) {
                    continue;
                }
                String secondID = new String(curatorClient.getData(snPath));
                secondServerCache.put(secondID, service.getServiceId());
            }
            System.out.println("addserviceMap:" + firstServerCache);
            System.out.println("addserviceMap:" + secondServerCache);

        } finally {
            if (curatorClient != null) {
                curatorClient.close();
            }
        }
    }

    /** 概述：加载所有关于该SN的2级SID对应的1级SID
     * @param service
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void addService(Service service) {
        // serverID信息加载
        firstServerCache.put(service.getServiceId(), service);
        CuratorClient curatorClient = null;
        try {
            curatorClient = CuratorClient.getClientInstance(zkHosts);
            List<String> firstIDs = curatorClient.getChildren(zkServerIDPath);
            for (String firstID : firstIDs) {
                String snPath = zkServerIDPath + SEPARATOR + firstID + SEPARATOR + snIndex;
                if (!curatorClient.checkExists(snPath)) {
                    continue;
                }
                String secondID = new String(curatorClient.getData(snPath));
                secondServerCache.put(secondID, firstID);
            }
        } finally {
            if (curatorClient != null) {
                curatorClient.close();
            }
        }
    }

    /** 概述：移除该SN对应的2级SID对应的1级SID
     * @param service
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void removeService(Service service) {
        firstServerCache.remove(service.getServiceId(), service);
        // 移除1级SID对应的2级SID
        for (Entry<String, String> entry : secondServerCache.entrySet()) {
            if (service.getServiceId().equals(entry.getValue())) {
                secondServerCache.remove(entry.getKey());
            }
        }

    }

    public ServiceMetaInfo getFirstServerCache(String SecondID) {
        System.out.println(firstServerCache);
        System.out.println(secondServerCache);
        return new ServiceMetaInfo() {

            @Override
            public Service getFirstServer() {
                String firstServerID = secondServerCache.get(SecondID);
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

    public Service getRandomService() {
        List<String> firstIDs = new ArrayList<String>(firstServerCache.keySet());
        Random random = new Random();
        String randomFirstID = firstIDs.get(random.nextInt(firstIDs.size()));
        return firstServerCache.get(randomFirstID);
    }

    public List<String> listSecondID() {
        return new ArrayList<String>(secondServerCache.keySet());
    }

    public List<String> listFirstID() {
        return new ArrayList<String>(firstServerCache.keySet());
    }

    public int getSnIndex() {
        return snIndex;
    }

    @Override
    public Map<String, Service> getServerCache() {
        return firstServerCache;
    }
}
