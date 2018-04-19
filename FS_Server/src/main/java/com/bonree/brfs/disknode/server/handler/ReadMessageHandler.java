package com.bonree.brfs.disknode.server.handler;

import java.io.IOException;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.DiskReader;

public class ReadMessageHandler implements MessageHandler {
	public static final String PARAM_READ_OFFSET = "offset";
	public static final String PARAM_READ_LENGTH = "length";
	
	private DiskContext diskContext;
	
	public ReadMessageHandler(DiskContext context) {
		this.diskContext = context;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
			String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
			int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
			int length = lengthParam == null ? Integer.MAX_VALUE : Integer.parseInt(lengthParam);
			
			DiskReader reader = new DiskReader(diskContext.getAbsoluteFilePath(msg.getPath()));
			byte[] data = reader.read(offset, length);
			result.setSuccess(true);
			result.setData(data);
		} catch (IOException e) {
			result.setSuccess(false);
			result.setCause(e);
		} finally {
			callback.completed(result);
		}
		
	}

}
