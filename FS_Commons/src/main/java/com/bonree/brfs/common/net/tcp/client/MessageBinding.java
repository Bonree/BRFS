package com.bonree.brfs.common.net.tcp.client;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;

public class MessageBinding {
	private final BaseMessage message;
	private final ResponseHandler<BaseResponse> handler;
	
	public MessageBinding(BaseMessage message, ResponseHandler<BaseResponse> handler) {
		this.message = message;
		this.handler = handler;
	}

	public BaseMessage getMessage() {
		return message;
	}

	public ResponseHandler<BaseResponse> getHandler() {
		return handler;
	}
}
