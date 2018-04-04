package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskWriterManager;
import com.bonree.brfs.disknode.server.DiskMessage;

public class CloseMessageHandler implements MessageHandler<DiskMessage> {
	private DiskWriterManager nodeManager;

	public CloseMessageHandler(DiskWriterManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		try {
			nodeManager.close(msg.getFilePath());
			result.setSuccess(true);
		} finally {
			callback.completed(result);
		}
	}

}
