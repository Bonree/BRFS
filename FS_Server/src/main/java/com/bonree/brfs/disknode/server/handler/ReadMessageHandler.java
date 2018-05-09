package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;

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
		
		String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
		String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
		int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
		int length = lengthParam == null ? Integer.MAX_VALUE : Integer.parseInt(lengthParam);
		
		byte[] data = DataFileReader.readFile(diskContext.getConcreteFilePath(msg.getPath()), offset, length);
		
		result.setSuccess(data == null ? false : true);
		result.setData(data);
		callback.completed(result);
	}

}
