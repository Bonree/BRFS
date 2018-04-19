package com.bonree.brfs.disknode.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.DiskWriterManager;
import com.bonree.brfs.disknode.InputEventCallback;
import com.bonree.brfs.disknode.InputResult;
import com.bonree.brfs.disknode.client.WriteResult;

public class WriteMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(WriteMessageHandler.class);
	
	private DiskContext diskContext;
	private DiskWriterManager nodeManager;
	
	public WriteMessageHandler(DiskContext diskContext, DiskWriterManager nodeManager) {
		this.diskContext = diskContext;
		this.nodeManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult handleResult = new HandleResult();
		
		try {
			String realPath = diskContext.getAbsoluteFilePath(msg.getPath());
			LOG.debug("WRITE [{}], data length[{}]", realPath, msg.getContent().length);
			
			if(msg.getContent().length == 0) {
				throw new IllegalArgumentException("Writing data is Empty!!");
			}
			
			WriteData item = ProtoStuffUtils.deserialize(msg.getContent(), WriteData.class);
			
			nodeManager.writeAsync(realPath, item, new InputEventCallback() {
				
				@Override
				public void error(Throwable t) {
					handleResult.setSuccess(false);
					handleResult.setCause(t);
					
					callback.completed(handleResult);
				}
				
				@Override
				public void complete(InputResult result) {
					handleResult.setSuccess(true);
					
					WriteResult writeResult = new WriteResult();
					writeResult.setOffset(result.getOffset());
					writeResult.setSize(result.getSize());
					try {
//						handleResult.setData(ProtoStuffUtils.serialize(writeResult));
						handleResult.setData(JsonUtils.toJsonBytes(writeResult));
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					callback.completed(handleResult);
				}
			});
		} catch (Exception e) {
			handleResult.setSuccess(false);
			handleResult.setCause(e);
			callback.completed(handleResult);
		}
	}

}
