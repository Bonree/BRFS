package com.bonree.brfs.duplication.storageregion.impl;

import javax.inject.Inject;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.ZookeeperPaths;
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
	
	@Inject
	public ZkStorageRegionIdBuilder(CuratorFramework client, ZookeeperPaths paths) {
		this.idBuilder = new ZkSequenceNumberBuilder(client.usingNamespace(paths.getBaseClusterName().substring(1)),
				ZKPaths.makePath(DefaultStorageRegionManager.DEFAULT_PATH_STORAGE_REGION_ROOT, DEFAULT_PATH_STORAGE_NAME_IDS));
	}

	@Override
	public int createRegionId() throws Exception {
		return idBuilder.nextSequenceNumber();
	}

}
