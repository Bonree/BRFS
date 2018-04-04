package com.bonree.brfs.disknode.server.handler;

import java.io.File;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskWriterManager;
import com.bonree.brfs.disknode.server.DiskMessage;

public class DeleteMessageHandler implements MessageHandler<DiskMessage> {
	private DiskWriterManager nodeManager;
	
	public DeleteMessageHandler(DiskWriterManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			nodeManager.close(msg.getFilePath());
			
			File targetFile = new File(msg.getFilePath());
			targetFile.delete();
			
			result.setSuccess(true);
		} finally {
			callback.completed(result);
		}
	}

}
