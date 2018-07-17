package com.bonree.brfs.duplication.storageregion.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.StorageConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameExistException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameRemoveException;
import com.bonree.brfs.duplication.storageregion.handler.StorageNameMessage;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
public class DefaultStorageRegionManager implements StorageRegionManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageRegionManager.class);
    
    private static final String DEFAULT_PATH_STORAGE_REGION_NODES = "nodes";
    
    private static final int DEFAULT_MAX_CACHE_SIZE = 100;
    private LoadingCache<String, Optional<StorageRegion>> storageRegionCache;
    private ConcurrentHashMap<Integer, StorageRegion> storageIdMap = new ConcurrentHashMap<Integer, StorageRegion>();

    private CuratorFramework zkClient;
    private PathChildrenCache childrenCache;

    private CopyOnWriteArrayList<StorageRegionStateListener> listeners = new CopyOnWriteArrayList<StorageRegionStateListener>();

    private StorageRegionIdBuilder idBuilder;

    public DefaultStorageRegionManager(CuratorFramework client, StorageRegionIdBuilder idBuilder) {
        this.zkClient = client;
        this.idBuilder = idBuilder;
        this.storageRegionCache = CacheBuilder.newBuilder()
        		.maximumSize(DEFAULT_MAX_CACHE_SIZE)
        		.build(new StorageRegionLoader());
        this.childrenCache = new PathChildrenCache(client,
        		ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES),
        		false);
    }

    @Override
    public void start() throws Exception {
        zkClient.createContainers(ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES));
        childrenCache.getListenable().addListener(new InnerStorageRegionStateListener());
        childrenCache.start();
    }

    @Override
    public void stop() throws Exception {
        childrenCache.close();
    }

    private StorageRegion getCachedNode(String regionName) {
        try {
            Optional<StorageRegion> optional = storageRegionCache.get(regionName);
            if (optional.isPresent()) {
                return optional.get();
            }

            // 如果没有值需要把空值无效化，这样下次查询可以重新获取，而不是用缓存的空值
            storageRegionCache.invalidate(regionName);
            return null;
        } catch (ExecutionException e) {
        }

        return null;
    }

    @Override
    public boolean exists(String regionName) {
        return getCachedNode(regionName) != null;
    }

    private static String buildRegionPath(String regionName) {
        return ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES, regionName);
    }
    
    private String getDefaultStorageDataTtl() {
    	return Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_DATA_TTL);
    }
    
    private long getDefaultFileCapacity() {
    	return Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_FILE_CAPACITY);
    }
    
    private String getDefaultFilePatitionDuration() {
    	return Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_FILE_PATITION_DURATION);
    }
    
    public int getDefaultStorageReplicateCount() {
    	return Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_REPLICATE_COUNT);
    }

    @Override
    public StorageRegion createStorageRegion(String regionName, Attributes attrs) throws StorageNameExistException {
        if (exists(regionName)) {
            throw new StorageNameExistException(regionName);
        }
        
        int replicateCount = attrs.getInt(StorageNameMessage.ATTR_REPLICATION, getDefaultStorageReplicateCount());
        String dataTtl = attrs.getString(StorageNameMessage.ATTR_TTL, getDefaultStorageDataTtl());
        long fileCapacity = attrs.getLong(StorageNameMessage.ATTR_FILE_CAPACITY, getDefaultFileCapacity());
        String filePatitionDuration = attrs.getString(StorageNameMessage.ATTR_FILE_PATITION_DURATION, getDefaultFilePatitionDuration());
        
        String regionPath = buildRegionPath(regionName);
        try {
            zkClient.create().forPath(regionPath, JsonUtils.toJsonBytes(StorageRegion.newBuilder()
            		.setName(regionName)
            		.setId(idBuilder.createRegionId())
            		.setCreateTime(System.currentTimeMillis())
            		.setEnable(true)
            		.setReplicateNum(replicateCount)
            		.setDataTtl(dataTtl)
            		.setFileCapacity(fileCapacity)
            		.setFilePartitionDuration(filePatitionDuration)
            		.build()));
            
            return findStorageRegionByName(regionName);
        } catch (NodeExistsException e) {
        	throw new StorageNameExistException(regionName);
        } catch (Exception e) {
            LOG.warn("create storage name node error", e);
        }

        Stat regionStat = null;
        try {
            regionStat = zkClient.checkExists().forPath(regionPath);
        } catch (Exception e) {
        	LOG.warn("get storage name node stats error", e);
        }

        if (regionStat != null) {
            byte[] idBytes = null;
            try {
                idBytes = zkClient.getData().forPath(regionPath);
            } catch (Exception e) {
            	LOG.warn("get storage name node[{}] data error", regionName, e);
            }

            if (idBytes != null) {
                throw new StorageNameExistException(regionName);
            }
        }

        return null;
    }

    @Override
    public boolean removeStorageRegion(String regionName) throws StorageNameNonexistentException, StorageNameRemoveException {
        StorageRegion node = findStorageRegionByName(regionName);
        if (node == null) {
            throw new StorageNameNonexistentException(regionName);
        }
        if (node.isEnable()) {
            throw new StorageNameRemoveException(regionName);
        }

        try {
            zkClient.delete().forPath(buildRegionPath(regionName));
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
        return storageIdMap.get(id);
    }

    private void refreshStorageIdMap() {
        storageIdMap.clear();
        List<ChildData> childList = childrenCache.getCurrentData();
        for (ChildData child : childList) {
            StorageRegion node = findStorageRegionByName(ZKPaths.getNodeFromPath(child.getPath()));
            if (node != null) {
                storageIdMap.put(node.getId(), node);
            }
        }
    }

    @Override
    public boolean updateStorageRegion(String regionName, Attributes attrs) throws StorageNameNonexistentException {
    	Preconditions.checkNotNull(attrs);
        if(attrs.isEmpty()) {
        	return false;
        }
        
        StorageRegion region = findStorageRegionByName(regionName);
        if(region == null) {
        	throw new StorageNameNonexistentException(regionName);
        }
        
        StorageRegion.Builder regionBuilder = StorageRegion.newBuilder(region);
        for(String attrName : attrs.getAttributeNames()) {
        	if (StorageNameMessage.ATTR_TTL.equals(attrName)) {
        		regionBuilder.setDataTtl(attrs.getString(StorageNameMessage.ATTR_TTL));
            } else if (StorageNameMessage.ATTR_ENABLE.equals(attrName)) {
            	regionBuilder.setEnable(attrs.getBoolean(StorageNameMessage.ATTR_ENABLE));
            } else if (StorageNameMessage.ATTR_FILE_CAPACITY.equals(attrName)) {
            	regionBuilder.setFileCapacity(attrs.getLong(StorageNameMessage.ATTR_FILE_CAPACITY));
            } else if (StorageNameMessage.ATTR_FILE_PATITION_DURATION.equals(attrName)) {
            	regionBuilder.setFilePartitionDuration(attrs.getString(StorageNameMessage.ATTR_FILE_PATITION_DURATION));
            }
        }
        
        try {
            zkClient.setData().forPath(buildRegionPath(regionName), JsonUtils.toJsonBytes(regionBuilder.build()));
        } catch (Exception e) {
        	LOG.warn("set storage name node[{}] data error", regionName, e);
            return false;
        }

        return true;
    }

    private class StorageRegionLoader extends CacheLoader<String, Optional<StorageRegion>> {

        @Override
        public Optional<StorageRegion> load(String regionName) throws Exception {
            StorageRegion region = null;
            try {
                region = JsonUtils.toObject(zkClient.getData().forPath(buildRegionPath(regionName)), StorageRegion.class);
            } catch (Exception e) {
            }

            return Optional.fromNullable(region);
        }

    }

    private class InnerStorageRegionStateListener implements PathChildrenCacheListener {

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
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionAdded(nodeOptional.get());
                            } catch (Exception e) {
                            }
                        }
                    }
                }
                    break;
                case CHILD_UPDATED: {
                    storageRegionCache.refresh(regionName);
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionUpdated(nodeOptional.get());
                            } catch (Exception e) {
                            }
                        }
                    }
                }
                    break;
                case CHILD_REMOVED: {
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    storageRegionCache.invalidate(regionName);
                    if (!listeners.isEmpty() && nodeOptional.isPresent()) {
                        for (StorageRegionStateListener listener : listeners) {
                            try {
                                listener.storageRegionUpdated(nodeOptional.get());
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
    public List<StorageRegion> getStorageRegionList() {
        return new ArrayList<StorageRegion>(storageIdMap.values());
    }

    @Override
    public void addStorageRegionStateListener(StorageRegionStateListener listener) {
        listeners.add(listener);
    }

}
