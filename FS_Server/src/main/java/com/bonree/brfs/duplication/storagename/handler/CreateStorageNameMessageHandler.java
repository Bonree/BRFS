package com.bonree.brfs.duplication.storagename.handler;

import java.io.IOException;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;

public class CreateStorageNameMessageHandler implements MessageHandler<StorageNameMessage> {
	
	private StorageNameManager storageNameManager;
	
	public CreateStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handle(StorageNameMessage msg, HandleResultCallback callback) {
		StorageNameNode node = storageNameManager.createStorageName(msg.getName(), msg.getReplicas(), msg.getTtl());
		
		HandleResult result = new HandleResult();
		if(node == null) {
			result.setSuccess(false);
			result.setData(BrStringUtils.toUtf8Bytes("errorCode:222"));
		} else {
			result.setSuccess(true);
			byte[] nodeBytes = null;
			try {
				nodeBytes = ProtoStuffUtils.serialize(node);
			} catch (IOException e) {
				e.printStackTrace();
			}
			result.setData(nodeBytes);
		}
		
		callback.completed(result);
		
	}

}
