package com.bonree.brfs.client.route;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月7日 下午5:07:17
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public class RouteRoleCache {

    private CuratorClient curatorClient;
    private String baseRoutePath;

    private int storageIndex;

    private Map<String, NormalRoute> normalRouteDetail;

    private Map<String, VirtualRoute> virtualRouteDetail;

    public RouteRoleCache(CuratorClient curatorClient, int storageIndex, String baseRoutePath) {
        this.curatorClient = curatorClient;
        this.storageIndex = storageIndex;
        this.baseRoutePath = baseRoutePath;
        virtualRouteDetail = new ConcurrentHashMap<>();
        normalRouteDetail = new ConcurrentHashMap<>();
        loadRouteRole();
    }

    private void loadRouteRole() {
            // load virtual id
            String virtualPath = baseRoutePath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + storageIndex;
            List<String> virtualNodes = curatorClient.getChildren(virtualPath);
            if (virtualNodes != null && !virtualNodes.isEmpty()) {
                for (String virtualNode : virtualNodes) {
                    String dataPath = virtualPath + Constants.SEPARATOR + virtualNode;
                    byte[] data = curatorClient.getData(dataPath);
                    VirtualRoute virtual = JsonUtils.toObject(data, VirtualRoute.class);
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
                    NormalRoute normal =JsonUtils.toObject(data, NormalRoute.class);
                    normalRouteDetail.put(normal.getSecondID(), normal);
                }
            }
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public Map<String, NormalRoute> getNormalRouteCache() {
        return normalRouteDetail;
    }

    public Map<String, VirtualRoute> getVirtualRouteCache() {
        return virtualRouteDetail;
    }

    public NormalRoute getRouteRole(String secondID) {
        return normalRouteDetail.get(secondID);
    }

    public VirtualRoute getVirtualRoute(String virtualID) {
        return virtualRouteDetail.get(virtualID);
    }
}
