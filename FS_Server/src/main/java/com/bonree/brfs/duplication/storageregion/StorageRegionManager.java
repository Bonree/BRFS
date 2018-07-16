package com.bonree.brfs.duplication.storageregion;

import java.util.List;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameExistException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.storageregion.exception.StorageNameRemoveException;

public interface StorageRegionManager extends LifeCycle {
	/**
	 * 判断有指定名称的StorageRegion是否存在
	 * 
	 * @param regionName
	 * @return
	 */
	boolean exists(String regionName);
	
	/**
	 * <p>创建StorageRegion节点。<p>
	 * 
	 * <p>如果已经存在，则返回已存在的节点；否则返回新建节点<p>
	 * 
	 * @param regionName StorageRegion名称
	 * @param properties 与StorageRegion相关的属性信息
	 * @return
	 * @throws StorageNameExistException 
	 */
	StorageRegion createStorageRegion(String regionName, Attributes properties) throws StorageNameExistException;
	
	/**
	 * <p>更新指定StorageRegion的属性信息<p>
	 * 
	 * @param regionName
	 * @param properties
	 * @return
	 * @throws StorageNameNonexistentException 
	 */
	boolean updateStorageRegion(String regionName, Attributes properties) throws StorageNameNonexistentException;
	
	/**
	 * 删除指定名称的StorageRegion节点
	 * 
	 * @param regionName
	 * @return
	 * @throws StorageNameNonexistentException 
	 * @throws StorageNameRemoveException 
	 */
	boolean removeStorageRegion(String regionName) throws StorageNameNonexistentException, StorageNameRemoveException;
	
	/**
	 * 查询指定名称的StorageRegion节点
	 * 
	 * @param regionName
	 * @return
	 */
	StorageRegion findStorageRegionByName(String regionName);
	
	/**
	 * 查询指定ID的StorageRegion节点
	 * 
	 * @param regionId
	 * @return
	 */
	StorageRegion findStorageRegionById(int regionId);
	
	/**
	 * 获取包含当前所有的StorageRegion节点列表
	 * 
	 * @return
	 */
	List<StorageRegion> getStorageRegionList();
	

	void addStorageRegionStateListener(StorageRegionStateListener listener);
}
