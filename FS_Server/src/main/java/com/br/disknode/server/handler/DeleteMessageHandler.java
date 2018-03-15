package com.br.disknode.server.handler;

import java.io.File;

import com.br.disknode.server.DiskMessage;
import com.br.disknode.server.DiskMessageHandler;
import com.br.disknode.server.HandleResult;
import com.br.disknode.server.HandleResultCallback;

public class DeleteMessageHandler implements DiskMessageHandler {

	@Override
	public void handle(DiskMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			File targetFile = new File(msg.getFilePath());
			targetFile.delete();
			
			result.setSuccess(true);
		} finally {
			callback.completed(result);
		}
	}

}
