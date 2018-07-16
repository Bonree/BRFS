package com.bonree.brfs.duplication.storageregion.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegionIdBuilder;

/**
 * 通过创建永久有序节点获取全局唯一ID
 * 
 * @author yupeng
 *
 */
public class ZkStorageRegionIdBuilder implements StorageRegionIdBuilder {
	private static final String DEFAULT_PATH_STORAGE_NAME_IDS = "ids";
	
	private SequenceNumberBuilder idBuilder;
	
	public ZkStorageRegionIdBuilder(CuratorFramework client) {
		this.idBuilder = new ZkSequenceNumberBuilder(client,
				ZKPaths.makePath(StorageRegionZkPaths.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_NAME_IDS));
	}

	@Override
	public short createRegionId() throws Exception {
		return (short) idBuilder.nextSequenceNumber();
	}

}
