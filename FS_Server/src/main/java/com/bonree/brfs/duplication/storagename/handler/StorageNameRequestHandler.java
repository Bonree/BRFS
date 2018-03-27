package com.bonree.brfs.duplication.storagename.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.server.handler.HandleResult;
import com.bonree.brfs.duplication.server.handler.HandleResultCallback;
import com.bonree.brfs.duplication.server.handler.MessageHandler;
import com.bonree.brfs.duplication.server.netty.NettyHttpRequestHandler;
import com.bonree.brfs.duplication.server.netty.ResponseSender;
import com.google.common.base.Splitter;

public class StorageNameRequestHandler implements NettyHttpRequestHandler {
	private static final Logger LOG = LoggerFactory.getLogger(StorageNameRequestHandler.class);
	
	private Map<String, MessageHandler<StorageNameMessage>> handlers = new HashMap<String, MessageHandler<StorageNameMessage>>();
	
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
		
		handler.handle(message, new DefaultHandleResultCallback(ctx));
	}
	
	private String parseName(String uri) {
		return Splitter.on('/').omitEmptyStrings().trimResults().splitToList(uri).get(0);
	}

	private class DefaultHandleResultCallback implements HandleResultCallback {
		private ChannelHandlerContext context;
		
		public DefaultHandleResultCallback(ChannelHandlerContext ctx) {
			this.context = ctx;
		}

		@Override
		public void completed(HandleResult result) {
			HttpResponseStatus status = HttpResponseStatus.OK;
			ByteBuf content = Unpooled.buffer(0);
			if(!result.isSuccess()) {
				status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
				if(result.getCause() != null) {
					content = Unpooled.wrappedBuffer(result.getCause().toString().getBytes(CharsetUtil.UTF_8));
				}
			}
			
			if(result.getData() != null) {
				content = Unpooled.wrappedBuffer(result.getData());
			}
			
			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
	        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
	        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
	        ResponseSender.sendResponse(context, response);
		}
		
	}
}
