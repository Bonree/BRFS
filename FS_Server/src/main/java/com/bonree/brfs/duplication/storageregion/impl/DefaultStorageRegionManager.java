package com.bonree.brfs.duplication.storageregion.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.StorageRegionStateListener;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameExistException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameRemoveException;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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
    
    private static final int DEFAULT_MAX_CACHE_SIZE = 128;
    private LoadingCache<String, Optional<StorageRegion>> storageRegionCache;
    private ConcurrentHashMap<Integer, StorageRegion> regionIds = new ConcurrentHashMap<Integer, StorageRegion>();
    
    private ExecutorService executor = Executors.newSingleThreadExecutor(new PooledThreadFactory("storage_region_state"));

    private CuratorFramework zkClient;
    private PathChildrenCache childrenCache;

    private CopyOnWriteArrayList<StorageRegionStateListener> listeners = new CopyOnWriteArrayList<StorageRegionStateListener>();

    private StorageRegionIdBuilder idBuilder;

    public DefaultStorageRegionManager(CuratorFramework client, StorageRegionIdBuilder idBuilder) {
        this.zkClient = client;
        this.idBuilder = idBuilder;
        this.storageRegionCache = CacheBuilder.newBuilder()
        		.maximumSize(DEFAULT_MAX_CACHE_SIZE)
        		.removalListener(new StorageRegionRemoveListener())
        		.build(new StorageRegionLoader());
        this.childrenCache = new PathChildrenCache(client,
        		ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES),
        		false);
    }

    @Override
    public void start() throws Exception {
        zkClient.createContainers(ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES));
        childrenCache.getListenable().addListener(new StorageRegionNodeStateListener());
        childrenCache.start();
    }

    @Override
    public void stop() throws Exception {
        childrenCache.close();
        executor.shutdown();
    }

    private StorageRegion getCachedNode(String regionName) {
        try {
        	return storageRegionCache.get(regionName).get();
        } catch (ExecutionException e) {
        	if(e.getCause() instanceof NoNodeException) {
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
        return ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_REGION_NODES, regionName);
    }

    @Override
    public StorageRegion createStorageRegion(String regionName, StorageRegionConfig config) throws Exception {
        if (exists(regionName)) {
            throw new StorageNameExistException(regionName);
        }
        
        String regionPath = buildRegionPath(regionName);
        try {
        	StorageRegion region = StorageRegion.newBuilder()
            		.setName(regionName)
            		.setId(idBuilder.createRegionId())
            		.setCreateTime(System.currentTimeMillis())
            		.setEnable(true)
            		.setReplicateNum(config.getReplicateNum())
            		.setDataTtl(config.getDataTtl())
            		.setFileCapacity(config.getFileCapacity())
            		.setFilePartitionDuration(config.getFilePartitionDuration())
            		.build();
        	
            zkClient.create().forPath(regionPath, JsonUtils.toJsonBytes(region));
            
            return region;
        } catch (NodeExistsException e) {
        	throw new StorageNameExistException(regionName);
        } catch (Exception e) {
            LOG.error("create storage name node error", e);
            throw e;
        }
    }
    
    @Override
    public void updateStorageRegion(String regionName, StorageRegionConfig config) throws Exception {
        StorageRegion region = findStorageRegionByName(regionName);
        if(region == null) {
        	throw new StorageNameNonexistentException(regionName);
        }
        
        try {
            zkClient.setData().forPath(buildRegionPath(regionName), JsonUtils.toJsonBytes(StorageRegion.newBuilder(region)
            		.setEnable(config.isEnable())
            		.setReplicateNum(config.getReplicateNum())
            		.setDataTtl(config.getDataTtl())
            		.setFileCapacity(config.getFileCapacity())
            		.setFilePartitionDuration(config.getFilePartitionDuration())
            		.build()));
        } catch (Exception e) {
        	LOG.warn("set storage name node[{}] data error", regionName, e);
            throw e;
        }
    }

    @Override
    public boolean removeStorageRegion(String regionName) throws Exception {
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
        return regionIds.get(id);
    }

    private class StorageRegionLoader extends CacheLoader<String, Optional<StorageRegion>> {

        @Override
        public Optional<StorageRegion> load(String regionName) throws Exception {
        	StorageRegion region = JsonUtils.toObject(zkClient.getData().forPath(buildRegionPath(regionName)), StorageRegion.class);
        	regionIds.put(region.getId(), region);
        	
            return Optional.of(region);
        }

    }
    
    private class StorageRegionRemoveListener implements RemovalListener<String, Optional<StorageRegion>> {

		@Override
		public void onRemoval(
				RemovalNotification<String, Optional<StorageRegion>> notification) {
			StorageRegion region = notification.getValue().get();
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
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    executor.submit(new Runnable() {
						
						@Override
						public void run() {
							if (!listeners.isEmpty() && nodeOptional.isPresent()) {
		                        for (StorageRegionStateListener listener : listeners) {
		                            try {
		                                listener.storageRegionAdded(nodeOptional.get());
		                            } catch (Exception e) {
		                            	LOG.error("notify region add error", e);
		                            }
		                        }
		                    }
						}
					});
                }
                    break;
                case CHILD_UPDATED: {
                    storageRegionCache.invalidate(regionName);
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    executor.submit(new Runnable() {
						
						@Override
						public void run() {
							if (!listeners.isEmpty() && nodeOptional.isPresent()) {
		                        for (StorageRegionStateListener listener : listeners) {
		                            try {
		                                listener.storageRegionUpdated(nodeOptional.get());
		                            } catch (Exception e) {
		                            	LOG.error("notify region update error", e);
		                            }
		                        }
		                    }
						}
					});
                }
                    break;
                case CHILD_REMOVED: {
                    Optional<StorageRegion> nodeOptional = storageRegionCache.get(regionName);
                    storageRegionCache.invalidate(regionName);
                    
                    executor.submit(new Runnable() {
						
						@Override
						public void run() {
							if (!listeners.isEmpty() && nodeOptional.isPresent()) {
		                        for (StorageRegionStateListener listener : listeners) {
		                            try {
		                                listener.storageRegionUpdated(nodeOptional.get());
		                            } catch (Exception e) {
		                            	LOG.error("notify region remove error", e);
		                            }
		                        }
		                    }
						}
					});
                }
                    break;
                default:
                    break;
            }
        }

    }

    @Override
    public List<StorageRegion> getStorageRegionList() {
        return new ArrayList<StorageRegion>(regionIds.values());
    }

    @Override
    public void addStorageRegionStateListener(StorageRegionStateListener listener) {
        listeners.add(listener);
    }

}
