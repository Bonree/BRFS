package com.bonree.brfs.duplication.storagename;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.StorageConfig;
import com.bonree.brfs.duplication.storagename.exception.StorageNameExistException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameRemoveException;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * StorageName信息管理类
 * 提供对StorageName的增删改查操作
 * 
 * *************************************************
 *    此类部分信息是通过zookeeper的通知机制实现的，实时性上
 *    可能存在不足
 * *************************************************
 * 
 * @author chen
 *
 */
public class DefaultStorageNameManager implements StorageNameManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageNameManager.class);

    private static final String DEFAULT_STORAGE_NAME_ROOT = ZookeeperPaths.STORAGE_NAMES;

//    private static final int DEFAULT_STORAGE_NAME_REPLICATIONS = 2;
//    private static final int DEFAULT_STORAGE_NAME_TTL = 100;

    private static final int DEFAULT_MAX_CACHE_SIZE = 100;
    private LoadingCache<String, Optional<StorageNameNode>> storageNameCache;
    private ConcurrentHashMap<Integer, StorageNameNode> storageIdMap = new ConcurrentHashMap<Integer, StorageNameNode>();

    private CuratorFramework zkClient;
    private PathChildrenCache childrenCache;
    
    private StorageConfig storageConfig;

    private CopyOnWriteArrayList<StorageNameStateListener> listeners = new CopyOnWriteArrayList<StorageNameStateListener>();

    private StorageIdBuilder idBuilder;

    public DefaultStorageNameManager(StorageConfig storageConfig,CuratorFramework client, StorageIdBuilder idBuilder) {
        this.storageConfig = storageConfig;
        this.zkClient = client;
        this.idBuilder = idBuilder;
        this.storageNameCache = CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE).build(new StorageNameNodeLoader());
        this.childrenCache = new PathChildrenCache(client, ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null), false);
    }

    @Override
    public void start() throws Exception {
        zkClient.createContainers(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null));
        childrenCache.getListenable().addListener(new InnerStorageNameStateListener());
        childrenCache.start();
    }

    @Override
    public void stop() throws Exception {
        childrenCache.close();
    }

    private StorageNameNode getCachedNode(String storageName) {
        try {
            Optional<StorageNameNode> optional = storageNameCache.get(storageName);
            if (optional.isPresent()) {
                return optional.get();
            }

            // 如果没有值需要把空值无效化，这样下次查询可以重新获取，而不是用缓存的空值
            storageNameCache.invalidate(storageName);
            return null;
        } catch (ExecutionException e) {
        }

        return null;
    }

    @Override
    public boolean exists(String storageName) {
        return getCachedNode(storageName) != null;
    }

    private static String buildStorageNamePath(String storageName) {
        return ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, storageName);
    }

    @Override
    public StorageNameNode createStorageName(String storageName, Attributes attrs) throws StorageNameExistException {
        if (exists(storageName)) {
            throw new StorageNameExistException(storageName);
        }
        
        StorageNameNode node = new StorageNameNode(storageName, idBuilder.createStorageId(), attrs.getInt(StorageNameNode.ATTR_REPLICATION, storageConfig.getReplication()), attrs.getInt(StorageNameNode.ATTR_TTL, storageConfig.getDataTtl()));
        String storageNamePath = buildStorageNamePath(storageName);

        String path = null;
        try {
            node.setCreateTime(System.currentTimeMillis());
            path = zkClient.create().forPath(storageNamePath, JsonUtils.toJsonBytes(node));
        } catch (Exception e) {
            LOG.warn("create storage name node error", e);
        }

        if (path != null) {
            return node;
        }

        Stat storagenNameStat = null;
        try {
            storagenNameStat = zkClient.checkExists().forPath(storageNamePath);
        } catch (Exception e) {
        	LOG.warn("get storage name node stats error", e);
        }

        if (storagenNameStat != null) {
            byte[] idBytes = null;
            try {
                idBytes = zkClient.getData().forPath(storageNamePath);
            } catch (Exception e) {
            	LOG.warn("get storage name node[{}] data error", storageName, e);
            }

            if (idBytes != null) {
                throw new StorageNameExistException(storageName);
            }
        }

        return null;
    }

    @Override
    public boolean removeStorageName(int storageId) throws StorageNameNonexistentException, StorageNameRemoveException {
        StorageNameNode node = findStorageName(storageId);
        if (node == null) {
            throw new StorageNameNonexistentException(storageId);
        }
        if (node.isEnable()) {
            throw new StorageNameRemoveException(storageId);
        }

        try {
            zkClient.delete().forPath(buildStorageNamePath(node.getName()));
        } catch (Exception e) {
        	LOG.warn("delete storage name node id[{}] error", storageId, e);
            return false;
        }

        return true;
    }

    @Override
    public boolean removeStorageName(String storageName) throws StorageNameNonexistentException, StorageNameRemoveException {
        StorageNameNode node = findStorageName(storageName);
        if (node == null) {
            throw new StorageNameNonexistentException(storageName);
        }
        if (node.isEnable()) {
            throw new StorageNameRemoveException(storageName);
        }

        try {
            zkClient.delete().forPath(buildStorageNamePath(storageName));
        } catch (Exception e) {
        	LOG.warn("delete storage name node name[{}] error", storageName, e);
            return false;
        }

        return true;
    }

    @Override
    public StorageNameNode findStorageName(String storageName) {
        return getCachedNode(storageName);
    }

    @Override
    public StorageNameNode findStorageName(int id) {
        return storageIdMap.get(id);
    }

    private void refreshStorageIdMap() {
        storageIdMap.clear();
        List<ChildData> childList = childrenCache.getCurrentData();
        for (ChildData child : childList) {
            StorageNameNode node = findStorageName(ZKPaths.getNodeFromPath(child.getPath()));
            if (node != null) {
                storageIdMap.put(node.getId(), node);
            }
        }
    }

    @Override
    public boolean updateStorageName(String storageName, Attributes attrs) throws StorageNameNonexistentException {
        if (!exists(storageName)) {
            throw new StorageNameNonexistentException(storageName);
        }

        StorageNameNode node = findStorageName(storageName);
        Set<String> attrNames = attrs.getAttributeNames();
        if (attrNames != null && !attrNames.isEmpty()) {
            Iterator<String> it = attrNames.iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (StorageNameNode.ATTR_TTL.equals(name)) {
                    node.setTtl(attrs.getInt(StorageNameNode.ATTR_TTL, node.getTtl()));
                } else if (StorageNameNode.ATTR_ENABLE.equals(name)) {
                    node.setEnable(attrs.getBoolean(StorageNameNode.ATTR_ENABLE));
                }
            }
        }
        try {
            zkClient.setData().forPath(buildStorageNamePath(storageName), JsonUtils.toJsonBytes(node));
        } catch (Exception e) {
        	LOG.warn("set storage name node[{}] data error", storageName, e);
            return false;
        }

        return true;
    }

    private class StorageNameNodeLoader extends CacheLoader<String, Optional<StorageNameNode>> {

        @Override
        public Optional<StorageNameNode> load(String storageName) throws Exception {
            StorageNameNode node = null;
            try {
                node = JsonUtils.toObject(zkClient.getData().forPath(buildStorageNamePath(storageName)), StorageNameNode.class);
            } catch (Exception e) {
            }

            return Optional.fromNullable(node);
        }

    }

    private class InnerStorageNameStateListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            ChildData data = event.getData();
            if (data == null) {
                return;
            }

            String storageName = ZKPaths.getNodeFromPath(data.getPath());
            LOG.info("event[{}] for storagename[{}]", event.getType(), storageName);
            switch (event.getType()) {
                case CHILD_ADDED: {
                    Optional<StorageNameNode> nodeOptional = storageNameCache.get(storageName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageNameStateListener listener : listeners) {
                            try {
                                listener.storageNameAdded(nodeOptional.get());
                            } catch (Exception e) {
                            }
                        }
                    }
                }
                    break;
                case CHILD_UPDATED: {
                    storageNameCache.refresh(storageName);
                    Optional<StorageNameNode> nodeOptional = storageNameCache.get(storageName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageNameStateListener listener : listeners) {
                            try {
                                listener.storageNameUpdated(nodeOptional.get());
                            } catch (Exception e) {
                            }
                        }
                    }
                }
                    break;
                case CHILD_REMOVED: {
                    Optional<StorageNameNode> nodeOptional = storageNameCache.get(storageName);
                    storageNameCache.invalidate(storageName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageNameStateListener listener : listeners) {
                            try {
                                listener.storageNameUpdated(nodeOptional.get());
                            } catch (Exception e) {
                            }
                        }
                    }
                }
                    break;
                default:
                    break;
            }

            refreshStorageIdMap();
        }

    }

    @Override
    public List<StorageNameNode> getStorageNameNodeList() {
        return new ArrayList<StorageNameNode>(storageIdMap.values());
    }

    @Override
    public void addStorageNameStateListener(StorageNameStateListener listener) {
        listeners.add(listener);
    }

}
