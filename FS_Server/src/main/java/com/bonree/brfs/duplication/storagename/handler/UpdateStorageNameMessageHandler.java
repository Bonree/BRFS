package com.bonree.brfs.duplication.storagename.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;

public class UpdateStorageNameMessageHandler extends StorageNameMessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(UpdateStorageNameMessageHandler.class);
	
	private StorageNameManager storageNameManager;
	
	public UpdateStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
		LOG.info("update storageName[{}], {}", msg.getName(), msg.getAttributes());
		boolean success = storageNameManager.updateStorageName(msg.getName(), msg.getAttributes());
		
		HandleResult result = new HandleResult();
		result.setSuccess(success);
		if(!success) {
			result.setData(BrStringUtils.toUtf8Bytes("errorCode:223"));
		}
		
		callback.completed(result);
	}

}
