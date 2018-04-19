package com.bonree.brfs.duplication.datastream.handler;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.ProtoStuffUtils;

public class ReadDataMessageHandler implements MessageHandler {

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		ReadDataMessage readMessage = ProtoStuffUtils.deserialize(msg.getContent(), ReadDataMessage.class);
	}

}
