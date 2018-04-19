package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;

public class UpdateStorageNameMessageHandler extends StorageNameMessageHandler {
	
	private StorageNameManager storageNameManager;
	
	public UpdateStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
		boolean success = storageNameManager.updateStorageName(msg.getName(), msg.getTtl());
		
		HandleResult result = new HandleResult();
		result.setSuccess(success);
		if(!success) {
			result.setData(BrStringUtils.toUtf8Bytes("errorCode:223"));
		}
		
		callback.completed(result);
	}

}
