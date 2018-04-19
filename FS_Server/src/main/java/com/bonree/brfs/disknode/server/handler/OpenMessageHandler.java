package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.DiskWriterManager;

public class OpenMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(OpenMessageHandler.class);
	
	public static final String PARAM_OVERRIDE = "override";
	
	private DiskContext diskContext;
	private DiskWriterManager nodeManager;
	
	public OpenMessageHandler(DiskContext context, DiskWriterManager nodeManager) {
		this.diskContext = context;
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		String overrideParam = msg.getParams().get(PARAM_OVERRIDE);
		boolean override = overrideParam == null ? false : true;
		
		try {
			String realPath = diskContext.getAbsoluteFilePath(msg.getPath());
			LOG.debug("OPEN [{}] override[{}]", realPath);
			nodeManager.buildDiskWriter(realPath, override);
			result.setSuccess(true);
		} catch (IOException e) {
			result.setSuccess(false);
			result.setCause(e);
		} finally {
			callback.completed(result);
		}
	}

}
