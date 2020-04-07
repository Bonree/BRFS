package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 17:56:29
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 路由器加载器
 ******************************************************************************/

public class SimpleRouteZKLoader implements RouteLoader {
    private String basePath = null;
    private CuratorFramework client = null;

    public SimpleRouteZKLoader(CuratorFramework client,String basePath) {
        this.basePath = basePath;
        this.client = client;
    }

    @Override
    public Collection<VirtualRoute> loadVirtualRoutes(int storageRegionId) throws Exception {
        String storageRegionPath = basePath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + storageRegionId;
        if(client.checkExists().forPath(storageRegionPath) == null){
            return null;
        }

        List<String> virtualRoutes = client.getChildren().forPath(storageRegionPath);
        if (virtualRoutes == null || virtualRoutes.isEmpty()) {
            return null;
        }
        Collection<VirtualRoute> routes = new ArrayList<>();
        for (String virtualNode : virtualRoutes) {
            String dataPath = storageRegionPath + Constants.SEPARATOR + virtualNode;
            byte[] data = client.getData().forPath(dataPath);
            VirtualRoute v = SingleRouteFactory.createVirtualRoute(data);
            if(v !=null){
                routes.add(v);
            }
        }
        return routes.isEmpty() ? null : routes;
    }

    @Override
    public Collection<NormalRouteInterface> loadNormalRoutes(int storageRegionId) throws Exception {
        String storageRegionPath = basePath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + storageRegionId;
        if(client.checkExists().forPath(storageRegionPath) == null){
            return null;
        }

        List<String> normalNodes = client.getChildren().forPath(storageRegionPath);
        if (normalNodes == null || normalNodes.isEmpty()) {
            return null;
        }
        Collection<NormalRouteInterface> routes = new ArrayList<>();
        for (String normalNode : normalNodes) {
            String dataPath = storageRegionPath + Constants.SEPARATOR + normalNode;
            byte[] data = client.getData().forPath(dataPath);
            NormalRouteInterface normal = SingleRouteFactory.createRoute(data);
            if(normal !=null){
                routes.add(normal);
            }
        }
        return routes.isEmpty() ? null : routes;
    }
}
