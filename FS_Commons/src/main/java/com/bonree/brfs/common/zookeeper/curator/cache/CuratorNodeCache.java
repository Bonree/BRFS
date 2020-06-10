package com.bonree.brfs.common.zookeeper.curator.cache;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorNodeCache {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorNodeCache.class);

    private Map<String, NodeCache> cacheMap = null;

    private CuratorFramework zkClient;

    CuratorNodeCache(CuratorFramework zkClient) {
        this.zkClient = zkClient;
        cacheMap = new ConcurrentHashMap<String, NodeCache>();
    }

    public void addListener(String path, NodeCacheListener listener) {
        LOG.info("add listener for path:" + path);
        NodeCache cache = cacheMap.get(path);
        if (cache == null) {
            cache = new NodeCache(zkClient, path);
            cacheMap.put(path, cache);
            startCache(path);
        }
        cache.getListenable().addListener(listener);
    }

    public void removeListener(String path, NodeCacheListener listener) {
        LOG.info("remove listener for path:" + path);
        NodeCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.getListenable().removeListener(listener);
        }
    }

    public void cancelListener(String path) throws IOException {
        LOG.info("cancel listener for path:" + path);
        NodeCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.close();
            cacheMap.remove(path);
        }
    }

    private void startCache(String path) {
        NodeCache cache = cacheMap.get(path);
        try {
            if (cache != null) {
                cache.start();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
