package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.HandleCallback;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;

public class PingPongMessageHandler implements MessageHandler {

	@Override
	public void handleMessage(BaseMessage baseMessage, HandleCallback callback) {
		callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.OK));
	}

}
