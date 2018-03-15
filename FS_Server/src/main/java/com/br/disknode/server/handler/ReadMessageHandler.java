package com.br.disknode.server.handler;

import java.util.Map;

import com.br.disknode.DiskReader;
import com.br.disknode.server.DiskMessage;
import com.br.disknode.server.DiskMessageHandler;
import com.br.disknode.server.HandleResult;
import com.br.disknode.server.HandleResultCallback;

public class ReadMessageHandler implements DiskMessageHandler {
	public static final String PARAM_READ_OFFSET = "read_offset";
	public static final String PARAM_READ_LENGTH = "read_length";

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			Map<String, Object> params = msg.getParams();
			int offset = (int) params.get(PARAM_READ_OFFSET);
			int length = (int) params.get(PARAM_READ_LENGTH);
			
			DiskReader reader = new DiskReader(msg.getFilePath());
			byte[] data = reader.read(offset, length);
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

}
