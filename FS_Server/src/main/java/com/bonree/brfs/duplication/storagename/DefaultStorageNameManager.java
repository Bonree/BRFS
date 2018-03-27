package com.bonree.brfs.duplication.storagename;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.utils.ProtoStuffUtils;

/**
 * 当前存在问题：
 * 1、通知机制的延迟导致数据不是立即可查
 * 2、快速的添加删除操作会导致通知机制触发不了
 * 
 * @author chen
 *
 */
public class DefaultStorageNameManager implements StorageNameManager {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageNameManager.class);
	
	private static final String DEFAULT_STORAGE_NAME_ROOT = "storageNames";
	
	private ConcurrentHashMap<String, StorageNameNode> storageNameMap = new ConcurrentHashMap<String, StorageNameNode>();
	private ConcurrentHashMap<Integer, StorageNameNode> storageIdMap = new ConcurrentHashMap<Integer, StorageNameNode>();
	
	private CuratorFramework zkClient;
	private PathChildrenCache cache;
	
	public DefaultStorageNameManager(CuratorFramework client) {
		this.zkClient = client;
		this.cache = new PathChildrenCache(client, ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null), true);
		this.cache.getListenable().addListener(new StorageNameStateListener());
	}

	@Override
	public void start() throws Exception {
		zkClient.createContainers(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null));
		cache.start();
	}

	@Override
	public void stop() throws Exception {
		cache.close();
	}

	@Override
	public boolean exists(String storageName) {
		return storageNameMap.containsKey(storageName);
	}

	@Override
	public StorageNameNode createStorageName(String storageName, int replicas, int ttl) {
		if(exists(storageName)) {
			return findStorageName(storageName);
		}
		
		StorageNameNode node = new StorageNameNode(storageName, StorageIdBuilder.createStorageId(), replicas, ttl);
		String storageNamePath = ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, storageName);
		
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
			zkClient.delete().forPath(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, node.getName()));
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
			zkClient.delete().forPath(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, storageName));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	@Override
	public StorageNameNode findStorageName(String storageName) {
		return storageNameMap.get(storageName);
	}

	@Override
	public StorageNameNode findStorageName(int id) {
		return storageIdMap.get(id);
	}
	
	private synchronized void addStorageNameNode(StorageNameNode node) {
		storageNameMap.put(node.getName(), node);
		storageIdMap.put(node.getId(), node);
	}
	
	private synchronized void deleteStorageNameNode(StorageNameNode node) {
		storageNameMap.remove(node.getName());
		storageIdMap.remove(node.getId());
	}

	private class StorageNameStateListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			LOG.info("EVENT--{}", event);
			ChildData data = event.getData();
			if(data != null) {
				StorageNameNode node = ProtoStuffUtils.deserialize(data.getData(), StorageNameNode.class);
				switch (event.getType()) {
				case CHILD_ADDED:
				case CHILD_UPDATED:
					LOG.info("ADD--{}", node);
					addStorageNameNode(node);
					break;
				case CHILD_REMOVED:
					LOG.info("REMOVE--{}", node);
					deleteStorageNameNode(node);
					break;
				default:
					break;
				}
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
			zkClient.setData().forPath(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, storageName), ProtoStuffUtils.serialize(node));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
}
