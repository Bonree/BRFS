package com.bonree.brfs.duplication.storagename.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameExistException;

public class CreateStorageNameMessageHandler extends StorageNameMessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CreateStorageNameMessageHandler.class);
	
	private StorageNameManager storageNameManager;
	
	public CreateStorageNameMessageHandler(StorageNameManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
		LOG.info("create storageName[{}]", msg.getName(), msg.getAttributes());
		HandleResult result = new HandleResult();
		StorageNameNode node = null;
		try{
			node = storageNameManager.createStorageName(msg.getName(), msg.getAttributes());
			LOG.info("created NODE[{}]", node);
		
		if(node == null) {
			result.setSuccess(false);
			result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_OPT_ERROR.name()));
		} else {
			result.setSuccess(true);
//			byte[] nodeBytes = null;
//			try {
//				nodeBytes = ProtoStuffUtils.serialize(node);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.SUCCESS.name()));
		}
		}catch (StorageNameExistException e) {
            result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_EXIST_ERROR.name()));
        }
		
		callback.completed(result);
	}

}
