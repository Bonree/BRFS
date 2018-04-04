package com.bonree.brfs.duplication.datastream.handler;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.http.netty.DefaultNettyHandleResultCallback;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.ResponseSender;
import com.bonree.brfs.common.utils.ProtoStuffUtils;

@Sharable
public class DuplicationRequestHandler implements NettyHttpRequestHandler<DataMessage> {
	private static final Logger LOG = LoggerFactory.getLogger(DuplicationRequestHandler.class);
	
	private Map<HttpMethod, MessageHandler<DataMessage>> methodToOps = new HashMap<HttpMethod, MessageHandler<DataMessage>>();
	
	@Override
	public void addMessageHandler(String method, MessageHandler<DataMessage> handler) {
		methodToOps.put(HttpMethod.valueOf(method), handler);
	}

	@Override
	public void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request) {
		LOG.info("handle request[{}:{}]", request.method(), request.uri());
		
		MessageHandler<DataMessage> handler = methodToOps.get(request.method());
		if(handler == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
			return;
		}
		
		LOG.info("get content length--" + request.content().readableBytes());
		byte[] data = new byte[request.content().readableBytes()];
		request.content().readBytes(data);
		
		DataMessage message = ProtoStuffUtils.deserialize(data, DataMessage.class);
		if(message == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST);
			return;
		}
		
		handler.handle(message, new DefaultNettyHandleResultCallback(ctx));
	}
}
