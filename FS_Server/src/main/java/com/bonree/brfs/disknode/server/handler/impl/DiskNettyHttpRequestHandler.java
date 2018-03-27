package com.bonree.brfs.disknode.server.handler.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.disknode.server.handler.DiskMessage;
import com.bonree.brfs.disknode.server.handler.HandleResult;
import com.bonree.brfs.disknode.server.handler.HandleResultCallback;
import com.bonree.brfs.disknode.server.netty.MessageHandler;
import com.bonree.brfs.disknode.server.netty.NettyHttpRequestHandler;
import com.bonree.brfs.disknode.server.netty.ResponseSender;

@Sharable
public class DiskNettyHttpRequestHandler implements NettyHttpRequestHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DiskNettyHttpRequestHandler.class);
	
	private Map<HttpMethod, MessageHandler<DiskMessage>> methodToOps = new HashMap<HttpMethod, MessageHandler<DiskMessage>>();
	
	public void put(String method, MessageHandler<DiskMessage> handler) {
		methodToOps.put(HttpMethod.valueOf(method), handler);
	}

	@Override
	public void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request) {
		LOG.debug("handle request[{}:{}]", request.method(), request.uri());
		
		MessageHandler<DiskMessage> handler = methodToOps.get(request.method());
		if(handler == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
			return;
		}
		
		QueryStringDecoder decoder = new QueryStringDecoder(request.uri(), CharsetUtil.UTF_8, true);
		
		DiskMessage message = new DiskMessage();
		message.setFilePath(decoder.path());
		
		Map<String, String> params = new HashMap<String, String>();
		for(String paramName : decoder.parameters().keySet()) {
			params.put(paramName, decoder.parameters().get(paramName).get(0));
		}
		message.setParams(params);
		
		byte[] data = new byte[request.content().readableBytes()];
		request.content().readBytes(data);
		message.setData(data);
		
		handler.handle(message, new DefaultHandleResultCallback(ctx));
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
			} else if(result.getData() != null) {
				content = Unpooled.wrappedBuffer(result.getData());
			}
			
			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
	        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
	        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
	        ResponseSender.sendResponse(context, response);
		}
		
	}
}
