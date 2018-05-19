package com.bonree.brfs.duplication.storagename;

import java.util.List;

import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.duplication.storagename.exception.StorageNameExistException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storagename.exception.StorageNameRemoveException;

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
	 * @throws StorageNameExistException 
	 */
	StorageNameNode createStorageName(String storageName, Attributes properties) throws StorageNameExistException;
	
	/**
	 * <p>更新指定StorageName的属性信息<p>
	 * 
	 * @param storageName
	 * @param ttl
	 * @return
	 * @throws StorageNameNonexistentException 
	 */
	boolean updateStorageName(String storageName, Attributes properties) throws StorageNameNonexistentException;
	
	/**
	 * 删除具有指定ID的StorageName节点
	 * 
	 * @param storageId
	 * @return
	 * @throws StorageNameNonexistentException 
	 * @throws StorageNameRemoveException 
	 */
	boolean removeStorageName(int storageId) throws StorageNameNonexistentException, StorageNameRemoveException;
	
	/**
	 * 删除指定名称的StorageName节点
	 * 
	 * @param storageName
	 * @return
	 * @throws StorageNameNonexistentException 
	 * @throws StorageNameRemoveException 
	 */
	boolean removeStorageName(String storageName) throws StorageNameNonexistentException, StorageNameRemoveException;
	
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
