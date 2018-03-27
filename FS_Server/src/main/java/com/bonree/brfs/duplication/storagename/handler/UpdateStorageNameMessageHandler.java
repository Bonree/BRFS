package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.duplication.server.handler.HandleResult;
import com.bonree.brfs.duplication.server.handler.HandleResultCallback;
import com.bonree.brfs.duplication.server.handler.MessageHandler;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.utils.StringUtils;

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
