package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;

public class UpdateStorageNameMessageHandler implements MessageHandler<StorageNameMessage> {
	
	private StorageNameManager storageNameManager;
	
	public UpdateStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handle(StorageNameMessage msg, HandleResultCallback callback) {
		boolean success = storageNameManager.updateStorageName(msg.getName(), msg.getTtl());
		
		HandleResult result = new HandleResult();
		result.setSuccess(success);
		if(!success) {
			result.setData(StringUtils.toUtf8Bytes("errorCode:223"));
		}
		
		callback.completed(result);
	}

}
