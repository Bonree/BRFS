package com.bonree.brfs.common.zookeeper.curator.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.base.Preconditions;

public class CuratorTreeCache {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorTreeCache.class);

    private static volatile CuratorTreeCache treeCache = null;

    private Map<String, TreeCache> cacheMap = null;

    private CuratorClient client = null;

    private CuratorTreeCache(String zkUrl) {
        client = CuratorClient.getClientInstance(zkUrl);
        cacheMap = new ConcurrentHashMap<String, TreeCache>();
    }

    public static CuratorTreeCache getTreeCacheInstance(String zkUrl) {
        LOG.info("create CuratorPathCache...");
        if (treeCache == null) {
            synchronized (CuratorTreeCache.class) {
                if (treeCache == null) {
                    treeCache = new CuratorTreeCache(Preconditions.checkNotNull(zkUrl, "zkUrl is not null!"));
                }
            }
        }
        return treeCache;
    }

    public void addListener(String path, AbstractTreeCacheListener listener) {
        LOG.info("add listener for tree:" + path);
        TreeCache cache = cacheMap.get(path);
        if (cache == null) {
            cache = new TreeCache(client.getInnerClient(), path);
            cacheMap.put(path, cache);
        }
        cache.getListenable().addListener(listener);
    }

    public void removeListener(String path, AbstractTreeCacheListener listener) {
        LOG.info("remove listener for tree:" + path);
        TreeCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.getListenable().removeListener(listener);
        }
    }

    public void startPathCache(String path) {
        TreeCache cache = cacheMap.get(path);
        try {
            if (cache != null) {
                cache.start();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
