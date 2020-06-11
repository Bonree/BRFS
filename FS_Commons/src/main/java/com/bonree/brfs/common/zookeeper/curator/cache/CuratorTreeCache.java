package com.bonree.brfs.common.zookeeper.curator.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorTreeCache {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorTreeCache.class);

    private Map<String, TreeCache> cacheMap = null;

    private CuratorFramework zkClient = null;

    CuratorTreeCache(CuratorFramework zkClient) {
        this.zkClient = zkClient;
        cacheMap = new ConcurrentHashMap<String, TreeCache>();
    }

    public void addListener(String path, TreeCacheListener listener) {
        LOG.info("add listener for tree:" + path);
        TreeCache cache = cacheMap.get(path);
        if (cache == null) {
            cache = new TreeCache(zkClient, path);
            cacheMap.put(path, cache);
            startCache(path);
        }
        cache.getListenable().addListener(listener);
    }

    public void removeListener(String path, TreeCacheListener listener) {
        LOG.info("remove listener for tree:" + path);
        TreeCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.getListenable().removeListener(listener);
        }
    }

    public void cancelListener(String path) {
        LOG.info("remove listeners for tree:" + path);
        TreeCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.close();
            cacheMap.remove(path);
        }
    }

    private void startCache(String path) {
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
