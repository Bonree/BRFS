package com.bonree.brfs.duplication.datastream.handler;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.ProtoStuffUtils;

public class DeleteDataMessageHandler implements MessageHandler {

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		DeleteDataMessage DeleteMessage = ProtoStuffUtils.deserialize(msg.getContent(), DeleteDataMessage.class);
	}

}
