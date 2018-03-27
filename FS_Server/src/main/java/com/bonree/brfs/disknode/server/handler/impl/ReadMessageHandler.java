package com.br.disknode.server.handler.impl;

import java.io.IOException;

import com.br.disknode.DiskReader;
import com.br.disknode.server.handler.DiskMessage;
import com.br.disknode.server.handler.HandleResult;
import com.br.disknode.server.handler.HandleResultCallback;
import com.br.disknode.server.netty.MessageHandler;

public class ReadMessageHandler implements MessageHandler<DiskMessage> {
	public static final String PARAM_READ_OFFSET = "read_offset";
	public static final String PARAM_READ_LENGTH = "read_length";

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			String offsetParam = msg.getParams().get(PARAM_READ_OFFSET);
			String lengthParam = msg.getParams().get(PARAM_READ_LENGTH);
			int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
			int length = lengthParam == null ? Integer.MAX_VALUE : Integer.parseInt(lengthParam);
			
			DiskReader reader = new DiskReader(msg.getFilePath());
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
