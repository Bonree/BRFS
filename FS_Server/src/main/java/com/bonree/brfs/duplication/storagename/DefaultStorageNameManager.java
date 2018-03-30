package com.bonree.brfs.duplication.storagename;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.google.common.base.Strings;

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
	
	public DefaultStorageNameManager(CuratorFramework client) {
		this.zkClient = client;
	}

	@Override
	public void start() throws Exception {
		zkClient.createContainers(ZKPaths.makePath(DEFAULT_STORAGE_NAME_ROOT, null));
	}

	@Override
	public void stop() throws Exception {
		//Nothing to do!
	}

	@Override
	public boolean exists(String storageName) {
		return storageNameMap.containsKey(storageName);
	}
	
	private String buildStorageNamePath(String storageName) {
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
		StorageNameNode node = storageNameMap.get(storageName);
		
		if(node == null) {
			node = findStorageNameFromZookeeper(storageName);
			
			if(node != null) {
				storageNameMap.put(storageName, node);
			}
		}
		
		return node;
	}
	
	private void refreshCache() {
		
	}
	
	private StorageNameNode findStorageNameFromZookeeper(String storageName) {
		try {
			return ProtoStuffUtils.deserialize(zkClient.getData().forPath(buildStorageNamePath(storageName)), StorageNameNode.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public StorageNameNode findStorageName(int id) {
		return storageIdMap.get(id);
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
