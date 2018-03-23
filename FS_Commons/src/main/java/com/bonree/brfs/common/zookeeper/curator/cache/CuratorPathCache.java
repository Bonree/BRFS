package com.bonree.brfs.common.zookeeper.curator.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class CuratorPathCache {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorPathCache.class);

    private static volatile CuratorPathCache pathCache = null;

    private Map<String, PathChildrenCache> cacheMap = null;

    private CuratorClient client = null;

    private CuratorPathCache(String zkUrl) {
        client = CuratorClient.getClientInstance(zkUrl);
        cacheMap = new ConcurrentHashMap<String, PathChildrenCache>();
    }

    public static CuratorPathCache getPathCacheInstance(String zkUrl) {
        LOG.info("create CuratorPathCache...");
        if (pathCache == null) {
            synchronized (CuratorPathCache.class) {
                if (pathCache == null) {
                    pathCache = new CuratorPathCache(zkUrl);
                }
            }
        }
        return pathCache;
    }

    public void addListener(String path, AbstractPathChildrenCacheListener listener) {
        LOG.info("add listener for path:" + path);
        PathChildrenCache cache = cacheMap.get(path);
        if (cache == null) {
            cache = new PathChildrenCache(client.getInnerClient(), path, true);
            cacheMap.put(path, cache);
        }
        cache.getListenable().addListener(listener);
    }

    public void removeListener(String path, AbstractPathChildrenCacheListener listener) {
        LOG.info("remove listener for path:" + path);
        PathChildrenCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.getListenable().removeListener(listener);
        }
    }

    public void startPathCache(String path) {
        PathChildrenCache cache = cacheMap.get(path);
        try {
            if (cache != null) {
                cache.start();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
