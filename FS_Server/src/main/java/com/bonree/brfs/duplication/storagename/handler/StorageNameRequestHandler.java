package com.bonree.brfs.duplication.storagename.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.http.netty.DefaultNettyHandleResultCallback;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.ResponseSender;
import com.google.common.base.Splitter;

public class StorageNameRequestHandler implements NettyHttpRequestHandler<StorageNameMessage> {
	private static final Logger LOG = LoggerFactory.getLogger(StorageNameRequestHandler.class);
	
	private Map<String, MessageHandler<StorageNameMessage>> handlers = new HashMap<String, MessageHandler<StorageNameMessage>>();
	
	@Override
	public void addMessageHandler(String method, MessageHandler<StorageNameMessage> handler) {
		handlers.put(method, handler);
	}

	@Override
	public void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request) {
		LOG.debug("handle request[{}:{}]", request.method(), request.uri());
		
		MessageHandler<StorageNameMessage> handler = handlers.get(request.method().name());
		if(handler == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
			return;
		}
		
		QueryStringDecoder decoder = new QueryStringDecoder(request.uri(), CharsetUtil.UTF_8, true);
		
		StorageNameMessage message = new StorageNameMessage();
		message.setName(parseName(decoder.path()));
		
		System.out.println("name----" + message.getName());
		
		byte[] data = new byte[request.content().readableBytes()];
		request.content().readBytes(data);
		//TODO set other properties of StorageNameMessage
		
		handler.handle(message, new DefaultNettyHandleResultCallback(ctx));
	}
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}
}
