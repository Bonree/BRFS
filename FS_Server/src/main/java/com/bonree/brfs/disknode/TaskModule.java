package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;

import static io.protostuff.CollectionSchema.MessageFactories.List;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月19日 21:49
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class TaskModule implements Module {
    @Override
    public void configure(Binder binder) {

    }

    @Provides
    @Singleton
    public RouteLoader getRouteLoader(CuratorFramework client, ZookeeperPaths zookeeperPaths){
        return new SimpleRouteZKLoader(client,zookeeperPaths.getBaseRoutePath());
    }
}
