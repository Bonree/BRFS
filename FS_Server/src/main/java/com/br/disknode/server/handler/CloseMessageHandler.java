package com.br.disknode.server.handler;

import java.io.IOException;

import com.br.disknode.DiskNodeManager;
import com.br.disknode.server.DiskMessage;
import com.br.disknode.server.DiskMessageHandler;
import com.br.disknode.server.HandleResult;
import com.br.disknode.server.HandleResultCallback;

public class CloseMessageHandler implements DiskMessageHandler {
	private DiskNodeManager nodeManager;
	
	public CloseMessageHandler(DiskNodeManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		try {
			nodeManager.close(msg.getFilePath());
			result.setSuccess(true);
		} catch (IOException e) {
			result.setSuccess(false);
			result.setCause(e);
		} finally {
			callback.completed(result);
		}
	}

}
