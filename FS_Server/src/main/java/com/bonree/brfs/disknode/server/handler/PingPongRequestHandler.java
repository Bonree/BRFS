package com.bonree.brfs.disknode.server.handler;

import io.netty.channel.ChannelHandler.Sharable;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;

@Sharable
public class PingPongRequestHandler implements MessageHandler {

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		result.setSuccess(true);
		
		callback.completed(result);
	}

}
