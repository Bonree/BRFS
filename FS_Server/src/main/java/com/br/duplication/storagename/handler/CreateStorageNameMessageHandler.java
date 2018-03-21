package com.br.duplication.storagename.handler;

import java.io.IOException;

import com.br.duplication.server.handler.HandleResult;
import com.br.duplication.server.handler.HandleResultCallback;
import com.br.duplication.server.handler.MessageHandler;
import com.br.duplication.storagename.StorageNameManager;
import com.br.duplication.storagename.StorageNameNode;
import com.br.duplication.utils.ProtoStuffUtils;
import com.br.duplication.utils.StringUtils;

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
			result.setData(StringUtils.toUtf8Bytes("errorCode:222"));
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
