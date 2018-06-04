package com.bonree.brfs.disknode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;

public class CloseMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CloseMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;

	public CloseMessageHandler(DiskContext context, FileWriterManager nodeManager) {
		this.diskContext = context;
		this.writerManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		try {
			LOG.info("CLOSE [{}]", msg.getPath());
			
			writerManager.close(diskContext.getConcreteFilePath(msg.getPath()));
			result.setSuccess(true);
		} finally {
			callback.completed(result);
		}
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return !message.getPath().isEmpty();
	}

}
