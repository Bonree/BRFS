package com.bonree.brfs.rebalance.route.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;

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

    public SimpleRouteZKLoader(CuratorFramework client, String basePath) {
        this.basePath = basePath;
        this.client = client;
    }

    @Inject
    public SimpleRouteZKLoader(CuratorFramework client, ZookeeperPaths zookeeperPaths) {
        this(client, zookeeperPaths.getBaseRoutePath());
    }

    @Override
    public Collection<VirtualRoute> loadVirtualRoutes(int storageRegionId) throws Exception {
        ImmutableList.Builder<VirtualRoute> result = ImmutableList.builder();
        String storageRegionPath =
            basePath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + storageRegionId;
        if (client.checkExists().forPath(storageRegionPath) == null) {
            return result.build();
        }

        List<String> virtualRoutes = client.getChildren().forPath(storageRegionPath);
        if (virtualRoutes == null || virtualRoutes.isEmpty()) {
            return result.build();
        }

        for (String virtualNode : virtualRoutes) {
            String dataPath = storageRegionPath + Constants.SEPARATOR + virtualNode;
            byte[] data = client.getData().forPath(dataPath);
            VirtualRoute v = SingleRouteFactory.createVirtualRoute(data);
            if (v != null) {
                result.add(v);
            }
        }
        return result.build();
    }

    @Override
    public Collection<NormalRouteInterface> loadNormalRoutes(int storageRegionId) throws Exception {
        ImmutableList.Builder<NormalRouteInterface> result = ImmutableList.builder();
        String storageRegionPath =
            basePath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + storageRegionId;
        if (client.checkExists().forPath(storageRegionPath) == null) {
            return result.build();
        }

        List<String> normalNodes = client.getChildren().forPath(storageRegionPath);
        if (normalNodes == null || normalNodes.isEmpty()) {
            return result.build();
        }

        for (String normalNode : normalNodes) {
            String dataPath = storageRegionPath + Constants.SEPARATOR + normalNode;
            byte[] data = client.getData().forPath(dataPath);
            NormalRouteInterface normal = SingleRouteFactory.createRoute(data);
            if (normal != null) {
                result.add(normal);
            }
        }
        return result.build();
    }
}
