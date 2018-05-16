package com.bonree.brfs.duplication.storagename;

import java.util.List;

import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.LifeCycle;

/**
 * 管理StorageName相关信息，可以对StorageName进行
 * 增、删、改、查
 * 
 * @author yupeng
 *
 */
public interface StorageNameManager extends LifeCycle {
	/**
	 * 判断有指定名称的StorageName是否存在
	 * 
	 * @param storageName
	 * @return
	 */
	boolean exists(String storageName);
	
	/**
	 * <p>创建StorageName节点。<p>
	 * 
	 * <p>如果已经存在，则返回已存在的节点；否则返回新建节点<p>
	 * 
	 * @param storageName StorageName名称
	 * @param properties 与StorageName相关的属性信息
	 * @return
	 */
	StorageNameNode createStorageName(String storageName, Attributes properties);
	
	/**
	 * <p>更新指定StorageName的属性信息<p>
	 * 
	 * @param storageName
	 * @param ttl
	 * @return
	 */
	boolean updateStorageName(String storageName, Attributes properties);
	
	/**
	 * 删除具有指定ID的StorageName节点
	 * 
	 * @param storageId
	 * @return
	 */
	boolean removeStorageName(int storageId);
	
	/**
	 * 删除指定名称的StorageName节点
	 * 
	 * @param storageName
	 * @return
	 */
	boolean removeStorageName(String storageName);
	
	/**
	 * 查询指定名称的StorageName节点
	 * 
	 * @param storageName
	 * @return
	 */
	StorageNameNode findStorageName(String storageName);
	
	/**
	 * 查询指定ID的StorageName节点
	 * 
	 * @param id
	 * @return
	 */
	StorageNameNode findStorageName(int id);
	
	/**
	 * 获取包含当前所有的StorageName节点列表
	 * 
	 * @return
	 */
	List<StorageNameNode> getStorageNameNodeList();
	
	void addStorageNameStateListener(StorageNameStateListener listener);
}
