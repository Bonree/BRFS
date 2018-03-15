package com.br.disknode.server.handler;

import java.io.IOException;
import java.util.Map;

import com.br.disknode.DiskNodeManager;
import com.br.disknode.server.DiskMessage;
import com.br.disknode.server.DiskMessageHandler;
import com.br.disknode.server.HandleResult;
import com.br.disknode.server.HandleResultCallback;

public class OpenMessageHandler implements DiskMessageHandler {
	public static final String PARAM_OVERRIDE = "override";
	
	private DiskNodeManager nodeManager;
	
	public OpenMessageHandler(DiskNodeManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		Map<String, Object> params = msg.getParams();
		boolean override = (boolean) params.get(PARAM_OVERRIDE);
		
		try {
			nodeManager.createWriter(msg.getFilePath(), override);
			result.setSuccess(true);
		} catch (IOException e) {
			result.setSuccess(false);
			result.setCause(e);
		} finally {
			callback.completed(result);
		}
	}

}
