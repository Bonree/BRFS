package com.bonree.brfs.duplication.storageregion.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.google.common.primitives.Ints;

public class OpenStorageRegionMessageHandler extends StorageRegionMessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenStorageRegionMessageHandler.class);
	
	private StorageRegionManager storageRegionManager;
	
	public OpenStorageRegionMessageHandler(StorageRegionManager storageRegionManager) {
		this.storageRegionManager = storageRegionManager;
	}

	@Override
	public void handleMessage(StorageRegionMessage msg, HandleResultCallback callback) {
		LOG.info("open storage region[{}]", msg.getName());
		StorageRegion node = storageRegionManager.findStorageRegionByName(msg.getName());
		if(node == null) {
			callback.completed(new HandleResult(false));
			return;
		}
		
		
		HandleResult result = new HandleResult();
		result.setSuccess(true);
		result.setData(Ints.toByteArray(node.getId()));
		callback.completed(result);
	}

}
