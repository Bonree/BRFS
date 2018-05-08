package com.bonree.brfs.client.route;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月7日 下午5:07:17
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 需要监听
 ******************************************************************************/
public class RouteRoleCache {

    private String zkHosts;

    private String baseRoutePath;

    private int storageIndex;

    private Map<String, NormalRoute> normalRouteDetail;

    private Map<String, VirtualRoute> virtualRouteDetail;

    public RouteRoleCache(String zkHosts, int storageIndex, String baseRoutePath) {
        this.zkHosts = zkHosts;
        this.storageIndex = storageIndex;
        this.baseRoutePath = baseRoutePath;
        virtualRouteDetail = new ConcurrentHashMap<>();
        normalRouteDetail = new ConcurrentHashMap<>();
        loadRouteRole();
    }

    private void loadRouteRole() {
        CuratorClient curatorClient = null;
        try {
            curatorClient = CuratorClient.getClientInstance(zkHosts);
            // load virtual id
            String virtualPath = baseRoutePath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + storageIndex;
            List<String> virtualNodes = curatorClient.getChildren(virtualPath);
            if (virtualNodes != null && !virtualNodes.isEmpty()) {
                for (String virtualNode : virtualNodes) {
                    String dataPath = virtualPath + Constants.SEPARATOR + virtualNode;
                    byte[] data = curatorClient.getData(dataPath);
                    VirtualRoute virtual = JSON.parseObject(data, VirtualRoute.class);
                    virtualRouteDetail.put(virtual.getVirtualID(), virtual);
                }
            }

            // load normal id
            String normalPath = baseRoutePath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + storageIndex;
            List<String> normalNodes = curatorClient.getChildren(normalPath);
            if (normalNodes != null && !normalNodes.isEmpty()) {
                for (String normalNode : normalNodes) {
                    String dataPath = normalPath + Constants.SEPARATOR + normalNode;
                    byte[] data = curatorClient.getData(dataPath);
                    NormalRoute normal = JSON.parseObject(data, NormalRoute.class);
                    normalRouteDetail.put(normal.getSecondID(), normal);
                }
            }
        } finally {
            if (curatorClient != null) {
                curatorClient.close();
            }
        }
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public NormalRoute getRouteRole(String secondID) {
        return normalRouteDetail.get(secondID);
    }

    public VirtualRoute getVirtualRoute(String virtualID) {
        return virtualRouteDetail.get(virtualID);
    }
    // TODO 少个监听
}
