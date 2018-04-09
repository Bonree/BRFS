package com.bonree.brfs.common.zookeeper.curator.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.base.Preconditions;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月9日 上午11:51:55
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 包括三种常用的cache，每个cache都为一个单例模式
 ******************************************************************************/
public class CuratorCacheFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorCacheFactory.class);

    private static volatile CuratorTreeCache treeCache = null;

    private static volatile CuratorNodeCache nodeCache = null;

    private static volatile CuratorPathCache pathCache = null;

    private static CuratorClient client = null;

    public synchronized static void init(String zkUrl) {
        if (client == null) {
            client = CuratorClient.getClientInstance(zkUrl);
        }
    }

    public static CuratorTreeCache getTreeCache() {
        LOG.info("create CuratorPathCache...");
        if (treeCache == null) {
            synchronized (CuratorTreeCache.class) {
                if (treeCache == null) {
                    treeCache = new CuratorTreeCache(Preconditions.checkNotNull(client, "CuratorCacheFactory is not init"));
                }
            }
        }
        return treeCache;
    }

    public static CuratorPathCache getPathCache() {
        LOG.info("create CuratorPathCache...");
        if (pathCache == null) {
            synchronized (CuratorPathCache.class) {
                if (pathCache == null) {
                    pathCache = new CuratorPathCache(Preconditions.checkNotNull(client, "zkUrl is not null!"));
                }
            }
        }
        return pathCache;
    }

    public static CuratorNodeCache getNodeCache() {
        LOG.info("create CuratorPathCache...");
        if (nodeCache == null) {
            synchronized (CuratorPathCache.class) {
                if (nodeCache == null) {
                    nodeCache = new CuratorNodeCache(Preconditions.checkNotNull(client, "zkUrl is not null!"));
                }
            }
        }
        return nodeCache;
    }

}
