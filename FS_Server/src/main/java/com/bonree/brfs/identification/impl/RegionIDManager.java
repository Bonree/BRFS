package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月31日 11:05:04
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: region获取id信息服务类，todo service类的payload增加磁盘节点信息，方便数据的读取
 ******************************************************************************/

public class RegionIDManager implements RouteLoader, SecondIdsInterface {
    private RouteLoader loader = null;
    private CuratorFramework client = null;
    private String secondIdBasePath = null;
    private String firstIdBasePath = null;

    public RegionIDManager(CuratorFramework client,String routeBasPath, String secondIdBasePath, String firstIdBasePath) {
        this.loader = new SimpleRouteZKLoader(client,routeBasPath);
        this.client = client;
        this.secondIdBasePath = secondIdBasePath;
        this.firstIdBasePath = firstIdBasePath;
    }

    @Override
    public Collection<String> getSecondIds(String serverId, int storageRegionId) {
        return null;
    }

    @Override
    public Collection<VirtualRoute> loadVirtualRoutes(int storageRegionId) throws Exception {
        return this.loader.loadVirtualRoutes(storageRegionId);
    }

    @Override
    public Collection<NormalRouteInterface> loadNormalRoutes(int storageRegionId) throws Exception {
        return this.loader.loadNormalRoutes(storageRegionId);
    }
}
