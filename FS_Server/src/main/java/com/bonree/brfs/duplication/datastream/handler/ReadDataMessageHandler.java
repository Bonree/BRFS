package com.bonree.brfs.duplication.datastream.handler;

import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;

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
