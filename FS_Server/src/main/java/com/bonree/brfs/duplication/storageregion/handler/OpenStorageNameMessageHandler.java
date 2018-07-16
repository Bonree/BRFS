package com.bonree.brfs.duplication.storageregion.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;

public class OpenStorageNameMessageHandler extends StorageNameMessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenStorageNameMessageHandler.class);
	
	private StorageRegionManager storageNameManager;
	
	public OpenStorageNameMessageHandler(StorageRegionManager storageNameManager) {
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handleMessage(StorageNameMessage msg, HandleResultCallback callback) {
		StorageRegion node = storageNameManager.findStorageRegionByName(msg.getName());
		
		
		HandleResult result = new HandleResult();
		if(node == null) {
		    result.setSuccess(false);
            result.setData(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_NONEXIST_ERROR.name()));
		} else {
			result.setSuccess(true);
			byte[] nodeBytes = null;
			try {
				nodeBytes = ProtoStuffUtils.serialize(node);
			} catch (IOException e) {
				LOG.error("serialize storage node error", e);
			}
			result.setData(BrStringUtils.toUtf8Bytes(String.valueOf(node.getId())));
		}
		
		callback.completed(result);
	}

}
