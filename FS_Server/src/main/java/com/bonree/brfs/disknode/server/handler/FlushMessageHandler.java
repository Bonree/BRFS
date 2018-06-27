package com.bonree.brfs.disknode.server.handler;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;

public class FlushMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(FlushMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	
	public FlushMessageHandler(DiskContext diskContext, FileWriterManager nodeManager) {
		this.diskContext = diskContext;
		this.writerManager = nodeManager;
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		String filePath = null;
		try {
			filePath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.info("flush file[{}]", filePath);
			writerManager.flushFile(filePath);
			
			result.setSuccess(true);
		} catch (FileNotFoundException e) {
			result.setSuccess(false);
		} finally {
			callback.completed(result);
		}
	}

}
