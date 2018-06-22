package com.bonree.brfs.disknode.server.handler;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;

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
	private ExecutorService threadPool;
	
	public FlushMessageHandler(DiskContext diskContext, FileWriterManager nodeManager, ExecutorService threadPool) {
		this.diskContext = diskContext;
		this.writerManager = nodeManager;
		this.threadPool = threadPool;
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				String filePath = diskContext.getConcreteFilePath(msg.getPath());
				
				HandleResult result = new HandleResult();
				try {
					LOG.info("flush file[{}]", filePath);
					writerManager.flushFile(filePath);
					
					result.setSuccess(true);
				} catch (FileNotFoundException e) {
					result.setSuccess(false);
				} finally {
					callback.completed(result);
				}
			}
		});
	}

}
