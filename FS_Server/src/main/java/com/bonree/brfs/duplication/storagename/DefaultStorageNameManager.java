package com.bonree.brfs.duplication.storagename;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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

import com.bonree.brfs.common.utils.ProtoStuffUtils;
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
	
	private static final String DEFAULT_STORAGE_NAME_ROOT = "storageNames";
	
	private static final int DEFAULT_MAX_CACHE_SIZE = 100;
	private LoadingCache<String, Optional<StorageNameNode>> storageNameCache;
	private ConcurrentHashMap<Integer, StorageNameNode> storageIdMap = new ConcurrentHashMap<Integer, StorageNameNode>();
	
	private CuratorFramework zkClient;
	private PathChildrenCache childrenCache;
	
	public DefaultStorageNameManager(CuratorFramework client) {
		this.zkClient = client;
		this.storageNameCache = CacheBuilder.newBuilder()
				                            .maximumSize(DEFAULT_MAX_CACHE_SIZE)
				                            .build(new StorageNameNodeLoader());
		this.childrenCache = new PathChildrenCache(client, ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null), false);
	}

	@Override
	public void start() throws Exception {
		zkClient.createContainers(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null));
		childrenCache.getListenable().addListener(new StorageNameStateListener());
		childrenCache.start();
	}

	@Override
	public void stop() throws Exception {
		childrenCache.close();
	}
	
	private StorageNameNode getCachedNode(String storageName) {
		try {
			Optional<StorageNameNode> optional = storageNameCache.get(storageName);
			if(optional.isPresent()) {
				return optional.get();
			}
			
			//如果没有值需要把空值无效化，这样下次查询可以重新获取，而不是用缓存的空值
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
	public StorageNameNode createStorageName(String storageName, int replicas, int ttl) {
		if(exists(storageName)) {
			return findStorageName(storageName);
		}
		
		StorageNameNode node = new StorageNameNode(storageName, StorageIdBuilder.createStorageId(), replicas, ttl);
		String storageNamePath = buildStorageNamePath(storageName);
		
		String path = null;
		try {
			path = zkClient.create().forPath(storageNamePath, ProtoStuffUtils.serialize(node));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(path != null) {
			return node;
		}
		
		Stat storagenNameStat = null;
		try {
			storagenNameStat = zkClient.checkExists().forPath(storageNamePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(storagenNameStat != null) {
			byte[] idBytes = null;
			try {
				idBytes = zkClient.getData().forPath(storageNamePath);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if(idBytes != null) {
				return ProtoStuffUtils.deserialize(idBytes, StorageNameNode.class);
			}
		}
		
		return null;
	}
	
	@Override
	public boolean removeStorageName(int storageId) {
		StorageNameNode node = findStorageName(storageId);
		if(node == null) {
			return true;
		}
		
		try {
			zkClient.delete().forPath(buildStorageNamePath(node.getName()));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	@Override
	public boolean removeStorageName(String storageName) {
		if(!exists(storageName)) {
			return true;
		}
		
		try {
			zkClient.delete().forPath(buildStorageNamePath(storageName));
		} catch (Exception e) {
			e.printStackTrace();
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
		for(ChildData child : childList) {
			StorageNameNode node = findStorageName(ZKPaths.getNodeFromPath(child.getPath()));
			if(node != null) {
				storageIdMap.put(node.getId(), node);
			}
		}
	}

	@Override
	public boolean updateStorageName(String storageName, int ttl) {
		if(!exists(storageName)) {
			return false;
		}
		
		StorageNameNode node = findStorageName(storageName);
		node.setTtl(ttl);
		
		try {
			zkClient.setData().forPath(buildStorageNamePath(storageName), ProtoStuffUtils.serialize(node));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private class StorageNameNodeLoader extends CacheLoader<String, Optional<StorageNameNode>> {

		@Override
		public Optional<StorageNameNode> load(String storageName) throws Exception {
			StorageNameNode node = null;
			try {
				node = ProtoStuffUtils.deserialize(zkClient.getData().forPath(buildStorageNamePath(storageName)), StorageNameNode.class);
			} catch (Exception e) {
			}
			
			return Optional.fromNullable(node);
		}
		
	}
	
	private class StorageNameStateListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			ChildData data = event.getData();
			String storageName = ZKPaths.getNodeFromPath(data.getPath());
			LOG.info("event[{}] for storagename[{}]", event.getType(), storageName);
			switch (event.getType()) {
			case CHILD_ADDED:
				storageNameCache.get(storageName);
				break;
			case CHILD_UPDATED:
				storageNameCache.refresh(storageName);
				break;
			case CHILD_REMOVED:
				storageNameCache.invalidate(storageName);
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
}
