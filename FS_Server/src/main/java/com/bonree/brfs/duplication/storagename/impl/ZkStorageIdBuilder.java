package com.bonree.brfs.duplication.storagename.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.duplication.storagename.StorageIdBuilder;

/**
 * 通过创建永久有序节点获取全局唯一ID
 * 
 * @author yupeng
 *
 */
public class ZkStorageIdBuilder implements StorageIdBuilder {
	private static final String DEFAULT_PATH_STORAGE_NAME_IDS = "ids";
	
	private SequenceNumberBuilder idBuilder;
	
	public ZkStorageIdBuilder(CuratorFramework client) {
		this.idBuilder = new ZkSequenceNumberBuilder(client,
				ZKPaths.makePath(StorageNameZkPaths.DEFAULT_PATH_STORAGE_NAME_ROOT, DEFAULT_PATH_STORAGE_NAME_IDS));
	}

	@Override
	public short createStorageId() {
		return (short) idBuilder.nextSequenceNumber();
	}

}
