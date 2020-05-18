package com.bonree.brfs.common.zookeeper.curator.cache;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorPathCache {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorPathCache.class);

    private Map<String, PathChildrenCache> cacheMap = null;

    private CuratorClient client = null;

    CuratorPathCache(CuratorClient client) {
        this.client = client;
        cacheMap = new ConcurrentHashMap<String, PathChildrenCache>();
    }

    public void addListener(String path, AbstractPathChildrenCacheListener listener) {
        LOG.info("add listener for path:" + path);
        PathChildrenCache cache = cacheMap.get(path);
        if (cache == null) {
            cache = new PathChildrenCache(client.getInnerClient(), path, true,
                                          new ThreadFactoryBuilder().setNameFormat(new File(path).getName()).build());
            cacheMap.put(path, cache);
            startCache(path);
        }
        cache.getListenable().addListener(listener);
    }

    public void removeListener(String path, AbstractPathChildrenCacheListener listener) {
        LOG.info("remove listener for path:" + path);
        PathChildrenCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.getListenable().removeListener(listener);
        }
        try {
            cache.close();
        } catch (IOException e) {
            LOG.error("close path child cache happen error {}", path, e);
        }
    }

    public void cancelListener(String path) throws IOException {
        LOG.info("cancel listener for path:" + path);
        PathChildrenCache cache = cacheMap.get(path);
        if (cache != null) {
            cache.close();
            cacheMap.remove(path);
        }
    }

    private void startCache(String path) {
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
