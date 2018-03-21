package com.br.duplication.storagename;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;

public class DefaultStorageNameManager implements StorageNameManager {
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
			path = zkClient.create().forPath(storageNamePath, node.toBytes());
		} catch (Exception e) {
		}
		
		if(path != null) {
			return node;
		}
		
		Stat storagenNameStat = null;
		try {
			storagenNameStat = zkClient.checkExists().forPath(storageNamePath);
		} catch (Exception e) {
		}
		
		if(storagenNameStat != null) {
			byte[] idBytes = null;
			try {
				idBytes = zkClient.getData().forPath(storageNamePath);
			} catch (Exception e) {
			}
			
			if(idBytes != null) {
				return StorageNameNode.fromBytes(idBytes);
			}
		}
		
		return null;
	}
	
	private void deleteZKNode(String storageName) throws Exception {
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
			ChildData data = event.getData();
			if(data != null) {
				StorageNameNode node = StorageNameNode.fromBytes(data.getData());
				switch (event.getType()) {
				case CHILD_ADDED:
				case CHILD_UPDATED:
					System.out.println("ADD-" + node);
					addStorageNameNode(node);
					break;
				case CHILD_REMOVED:
					System.out.println("DEL-" + node);
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
		// TODO Auto-generated method stub
		return false;
	}
}
