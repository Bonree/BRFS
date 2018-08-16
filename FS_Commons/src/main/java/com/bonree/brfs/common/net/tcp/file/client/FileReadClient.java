package com.bonree.brfs.common.net.tcp.file.client;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.client.AbstractTcpClient;
import com.bonree.brfs.common.net.tcp.client.ResponseHandler;
import com.bonree.brfs.common.net.tcp.file.ReadObject;

public class FileReadClient extends AbstractTcpClient<ReadObject, FileContentPart> {
	private static final Logger LOG = LoggerFactory.getLogger(FileReadClient.class);
	
	private ResponseHandler<FileContentPart> currentHandler;

	FileReadClient() {
		super();
	}
	
	FileReadClient(Executor executor) {
		super(executor);
	}

	@Override
	protected void handle(int token, FileContentPart response) {
//		LOG.info("handle response token[{}]", token);
		if(currentHandler == null) {
			currentHandler = takeHandler(token);
		}
		
		if(currentHandler == null) {
			throw new IllegalStateException("no handler to handle file content!");
		}
		
		final ResponseHandler<FileContentPart> handler = currentHandler;
		
		if(response.endOfContent()) {
			currentHandler = null;
		}
		
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				if(response.content() == null) {
					handler.error(new Exception("no content is find!"));
					return;
				}
				
				handler.handle(response);
			}
		});
	}

}
