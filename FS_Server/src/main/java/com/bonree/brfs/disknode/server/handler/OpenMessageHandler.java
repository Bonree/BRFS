package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskWriterManager;
import com.bonree.brfs.disknode.server.DiskMessage;

public class OpenMessageHandler implements MessageHandler<DiskMessage> {
	private static final Logger LOG = LoggerFactory.getLogger(OpenMessageHandler.class);
	
	public static final String PARAM_OVERRIDE = "override";
	
	private DiskWriterManager nodeManager;
	
	public OpenMessageHandler(DiskWriterManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		String overrideParam = msg.getParams().get(PARAM_OVERRIDE);
		boolean override = overrideParam == null ? false : true;
		
		try {
			LOG.debug("OPEN [{}] override[{}]", msg.getFilePath(), override);
			nodeManager.buildDiskWriter(msg.getFilePath(), override);
			result.setSuccess(true);
		} catch (IOException e) {
			result.setSuccess(false);
			result.setCause(e);
		} finally {
			callback.completed(result);
		}
	}

}
