package com.bonree.brfs.disknode.server.handler;

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;

public class ReadMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ReadMessageHandler.class);
	
	public static final String PARAM_READ_OFFSET = "offset";
	public static final String PARAM_READ_LENGTH = "size";
	
	private DiskContext diskContext;
	private ExecutorService threadPool;
	
	public ReadMessageHandler(DiskContext context, ExecutorService threadPool) {
		this.diskContext = context;
		this.threadPool = threadPool;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				HandleResult result = new HandleResult();
				
				try {
					String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
					String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
					int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
					int length = lengthParam == null ? Integer.MAX_VALUE : Integer.parseInt(lengthParam);
					
					LOG.info("read data offset[{}], size[{}]", offset, length);
					
					byte[] data = DataFileReader.readFile(diskContext.getConcreteFilePath(msg.getPath()), offset, length);
					
					result.setSuccess(data.length == 0 ? false : true);
					result.setData(data);
				} catch (Exception e) {
					LOG.error("read message error", e);
					result.setSuccess(false);
				} finally {
					callback.completed(result);
				}
				
			}
		});
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

}
