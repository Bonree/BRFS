package com.bonree.brfs.client.route.listener;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.shaded.com.google.common.base.Splitter;
import org.apache.curator.shaded.com.google.common.collect.Lists;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.client.route.RouteRoleCache;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月8日 上午9:48:26
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 监听某个cache
 ******************************************************************************/
public class RouteCacheListener implements TreeCacheListener {

    private RouteRoleCache routeRoleCache;

    public RouteCacheListener(RouteRoleCache routeRoleCache) {
        this.routeRoleCache = routeRoleCache;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        // 过滤事件，并不是所有的事件都得响应
        if (event.getType().equals(TreeCacheEvent.Type.NODE_ADDED)
        		&& event.getData() != null
        		&& event.getData().getData() != null
        		&& event.getData().getData().length > 0) {
            String path = event.getData().getPath();
            List<String> splitPaths = Lists.newArrayList(Splitter.on(Constants.SEPARATOR).omitEmptyStrings().trimResults().split(path));
            String endStr = splitPaths.get(splitPaths.size() - 1);
            if (endStr.substring(0, Constants.ROUTE_NODE.length()).equals(Constants.ROUTE_NODE) && splitPaths.size() > 4) {
                if (splitPaths.contains(Constants.VIRTUAL_ROUTE)) {
                    VirtualRoute route = JSON.parseObject(event.getData().getData(), VirtualRoute.class);
                    routeRoleCache.getVirtualRouteCache().put(route.getVirtualID(), route);
                } else if (splitPaths.contains(Constants.NORMAL_ROUTE)) {
                    NormalRoute route = JSON.parseObject(event.getData().getData(), NormalRoute.class);
                    routeRoleCache.getNormalRouteCache().put(route.getSecondID(), route);
                }
            }
        }
    }
}
