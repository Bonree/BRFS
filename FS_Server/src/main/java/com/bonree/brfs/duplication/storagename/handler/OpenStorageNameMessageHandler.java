package com.bonree.brfs.duplication.storagename.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;

public class OpenStorageNameMessageHandler extends StorageNameMessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenStorageNameMessageHandler.class);
	
	private StorageNameManager storageNameManager;
	
	public OpenStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
		StorageNameNode node = storageNameManager.findStorageName(msg.getName());
		
		
		HandleResult result = new HandleResult();
		if(node == null) {
			result.setSuccess(false);
			result.setData(BrStringUtils.toUtf8Bytes("errorCode:225"));
		} else {
			result.setSuccess(true);
			byte[] nodeBytes = null;
			try {
				nodeBytes = ProtoStuffUtils.serialize(node);
			} catch (IOException e) {
				e.printStackTrace();
			}
			result.setData(BrStringUtils.toUtf8Bytes(String.valueOf(node.getId())));
		}
		
		callback.completed(result);
	}

}
