package com.bonree.brfs.duplication.datastream.handler;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;

public class ReadDataMessageHandler implements MessageHandler {

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		String fileId = msg.getPath();
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return false;
	}

}
