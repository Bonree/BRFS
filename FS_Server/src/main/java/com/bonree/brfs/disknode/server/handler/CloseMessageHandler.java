package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.DiskWriterManager;

public class CloseMessageHandler implements MessageHandler {
	
	private DiskContext diskContext;
	private DiskWriterManager nodeManager;

	public CloseMessageHandler(DiskContext context, DiskWriterManager nodeManager) {
		this.diskContext = context;
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		try {
			nodeManager.close(diskContext.getAbsoluteFilePath(msg.getPath()));
			result.setSuccess(true);
		} finally {
			callback.completed(result);
		}
	}

}
