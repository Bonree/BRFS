package com.bonree.brfs.client.route.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import com.bonree.brfs.client.route.RouteRoleCache;

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
        // 过滤时间，并不是所有的事件都得响应

    }

}
