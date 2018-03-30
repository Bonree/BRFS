package com.bonree.brfs.duplication.storagename.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;

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
