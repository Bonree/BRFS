package com.bonree.brfs.duplication.storageregion.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionProperties;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionExistedException;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionNonexistentException;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionStateException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageName信息管理类 提供对StorageName的增删改查操作
 * <p>
 * *************************************************
 * 此类部分信息是通过zookeeper的通知机制实现的，实时性上 可能存在不足
 * *************************************************
 *
 * @author chen
 */
@ManageLifecycle
public class DefaultStorageRegionManager implements StorageRegionManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageRegionManager.class);

    public static final String DEFAULT_PATH_STORAGE_REGION_ROOT = "storageName";
    private static final String DEFAULT_PATH_STORAGE_REGION_NODES = "nodes";

    private static final int DEFAULT_MAX_CACHE_SIZE = 128;
    private final LoadingCache<String, StorageRegion> storageRegionCache;
    private final ConcurrentHashMap<Integer, StorageRegion> regionIds = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors
        .newSingleThreadExecutor(new PooledThreadFactory("storage_region_state"));

    private final CuratorFramework zkClient;
    private final PathChildrenCache childrenCache;

    private final CopyOnWriteArrayList<StorageRegionStateListener> listeners = new CopyOnWriteArrayList<>();

    private final StorageRegionIdBuilder idBuilder;

    @Inject
    public DefaultStorageRegionManager(CuratorFramework client,
                                       ZookeeperPaths paths,
                                       StorageRegionIdBuilder idBuilder) {
        this.zkClient = client.usingNamespace(paths.getBaseClusterName().substring(1));
        this.idBuilder = idBuilder;
        this.storageRegionCache = CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_MAX_CACHE_SIZE)
            .expireAfterWrite(10, TimeUnit.SECONDS)
            .refreshAfterWrite(10, TimeUnit.SECONDS)
            .removalListener(new StorageRegionRemoveListener())
            .build(new StorageRegionLoader());
        this.childrenCache = new PathChildrenCache(zkClient,
                                                   ZKPaths.makePath(DEFAULT_PATH_STORAGE_REGION_ROOT,
                                                                    DEFAULT_PATH_STORAGE_REGION_NODES),
                                                   true);
    }

    @LifecycleStart
    public void start() throws Exception {
        zkClient.createContainers(
            ZKPaths.makePath(DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES));
        childrenCache.getListenable().addListener(new StorageRegionNodeStateListener());
        childrenCache.start();
    }

    @LifecycleStop
    public void stop() throws Exception {
        childrenCache.close();
        executor.shutdown();
    }

    private StorageRegion getCachedNode(String regionName) {
        try {
            return storageRegionCache.get(regionName);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoNodeException) {
                LOG.warn("No storage region[{}] exists.", regionName);
                return null;
            }

            LOG.error("load storage region error", e);
        }

        return null;
    }

    @Override
    public boolean exists(String regionName) {
        return getCachedNode(regionName) != null;
    }

    private static String buildRegionPath(String regionName) {
        return ZKPaths.makePath(DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES, regionName);
    }

    @Override
    public StorageRegion createStorageRegion(String regionName, StorageRegionProperties props) throws Exception {
        if (exists(regionName)) {
            throw new StorageRegionExistedException(regionName);
        }

        String regionPath = buildRegionPath(regionName);
        try {
            StorageRegion region = new StorageRegion(
                regionName,
                idBuilder.createRegionId(),
                System.currentTimeMillis(),
                props);

            zkClient.create().forPath(regionPath, JsonUtils.toJsonBytes(region));

            return region;
        } catch (NodeExistsException e) {
            throw new StorageRegionExistedException(regionName);
        } catch (Exception e) {
            LOG.error("create storage name node error", e);
            throw e;
        }
    }

    @Override
    public void updateStorageRegion(String regionName, Properties props) throws Exception {
        StorageRegion region = findStorageRegionByName(regionName);
        if (region == null) {
            throw new StorageRegionNonexistentException(regionName);
        }

        try {
            zkClient.setData().forPath(
                buildRegionPath(regionName),
                JsonUtils.toJsonBytes(
                    new StorageRegion(
                        region.getName(),
                        region.getId(),
                        region.getCreateTime(),
                        region.getProperties().override(props))));
        } catch (Exception e) {
            LOG.warn("set storage name node[{}] data error", regionName, e);
            throw e;
        } finally {
            storageRegionCache.refresh(regionName);
        }
    }

    @Override
    public boolean removeStorageRegion(String regionName) throws Exception {
        StorageRegion node = findStorageRegionByName(regionName);
        if (node == null) {
            throw new StorageRegionNonexistentException(regionName);
        }

        if (node.isEnable()) {
            throw new StorageRegionStateException(regionName, "disabled", "enabled");
        }

        try {
            zkClient.delete().forPath(buildRegionPath(regionName));
            storageRegionCache.invalidate(regionName);
        } catch (Exception e) {
            LOG.warn("delete storage name node name[{}] error", regionName, e);
            return false;
        }

        return true;
    }

    @Override
    public StorageRegion findStorageRegionByName(String regionName) {
        return getCachedNode(regionName);
    }

    @Override
    public StorageRegion findStorageRegionById(int id) {
        return regionIds.get(id);
    }

    private class StorageRegionLoader extends CacheLoader<String, StorageRegion> {

        @Override
        @ParametersAreNonnullByDefault
        public StorageRegion load(String regionName) throws Exception {
            StorageRegion region = JsonUtils.toObject(
                zkClient.getData().forPath(buildRegionPath(regionName)),
                StorageRegion.class);
            regionIds.put(region.getId(), region);

            return region;
        }

    }

    private class StorageRegionRemoveListener implements RemovalListener<String, StorageRegion> {

        @Override
        public void onRemoval(RemovalNotification<String, StorageRegion> notification) {
            StorageRegion region = notification.getValue();
            regionIds.remove(region.getId());
        }

    }

    private class StorageRegionNodeStateListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            ChildData data = event.getData();
            if (data == null) {
                return;
            }

            String regionName = ZKPaths.getNodeFromPath(data.getPath());
            LOG.info("event[{}] for storage region[{}]", event.getType(), regionName);
            switch (event.getType()) {
            case CHILD_ADDED: {
                StorageRegion storageRegion = getCachedNode(regionName);
                if (storageRegion == null) {
                    LOG.error("No storage region[{}] is found", regionName);
                    return;
                }

                executor.submit(() -> {
                    if (!listeners.isEmpty()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionAdded(storageRegion);
                            } catch (Exception e) {
                                LOG.error("notify region add error", e);
                            }
                        }
                    }
                });

                break;
            }
            case CHILD_UPDATED: {
                storageRegionCache.invalidate(regionName);
                StorageRegion storageRegion = getCachedNode(regionName);
                if (storageRegion == null) {
                    LOG.error("No storage region[{}] is found", regionName);
                    return;
                }

                executor.submit(() -> {
                    if (!listeners.isEmpty()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionUpdated(storageRegion);
                            } catch (Exception e) {
                                LOG.error("notify region update error", e);
                            }
                        }
                    }
                });

                break;
            }
            case CHILD_REMOVED: {
                storageRegionCache.invalidate(regionName);
                StorageRegion storageRegion = JsonUtils.toObject(data.getData(), StorageRegion.class);

                executor.submit(() -> {
                    if (!listeners.isEmpty()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionRemoved(storageRegion);
                            } catch (Exception e) {
                                LOG.error("notify region remove error", e);
                            }
                        }
                    }
                });

                break;
            }
            default:
                break;
            }
        }

    }

    @Override
    public List<StorageRegion> getStorageRegionList() {
        return childrenCache.getCurrentData()
            .stream()
            .map(c -> getCachedNode(ZKPaths.getNodeFromPath(c.getPath())))
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public void addStorageRegionStateListener(StorageRegionStateListener listener) {
        listeners.add(listener);
    }

}
