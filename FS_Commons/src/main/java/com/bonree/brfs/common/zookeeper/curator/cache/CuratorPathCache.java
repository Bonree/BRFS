package com.bonree.brfs.common.zookeeper.curator.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.base.Function;

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

    public List<AbstractPathChildrenCacheListener> getAllListener(String path) {
        final List<AbstractPathChildrenCacheListener> list;
        PathChildrenCache cache = cacheMap.get(path);
        if (cache != null) {
            list = new ArrayList<AbstractPathChildrenCacheListener>();
            ListenerContainer<PathChildrenCacheListener> listeners = cache.getListenable();
            listeners.forEach(new Function<PathChildrenCacheListener, Void>() {
                @Override
                public Void apply(PathChildrenCacheListener input) {
                    list.add((AbstractPathChildrenCacheListener) input);
                    return null;
                }
            });
            return list;
        }

        return null;
    }

}
