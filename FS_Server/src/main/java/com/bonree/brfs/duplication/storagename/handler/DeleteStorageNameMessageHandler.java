package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.duplication.server.handler.HandleResult;
import com.bonree.brfs.duplication.server.handler.HandleResultCallback;
import com.bonree.brfs.duplication.server.handler.MessageHandler;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.utils.StringUtils;

public class DeleteStorageNameMessageHandler implements MessageHandler<StorageNameMessage> {
	
	private StorageNameManager storageNameManager;
	
	public DeleteStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handle(StorageNameMessage msg, HandleResultCallback callback) {
		boolean deleted = storageNameManager.removeStorageName(msg.getName());
		
		HandleResult result = new HandleResult();
		result.setSuccess(deleted);
		if(!deleted) {
			result.setData(StringUtils.toUtf8Bytes("errorCode:224"));
		}
		
		callback.completed(result);
	}

}
