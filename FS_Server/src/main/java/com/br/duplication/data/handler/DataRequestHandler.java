package com.br.duplication.data.handler;

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

import com.br.duplication.server.handler.HandleResult;
import com.br.duplication.server.handler.HandleResultCallback;
import com.br.duplication.server.handler.MessageHandler;
import com.br.duplication.server.netty.NettyHttpRequestHandler;
import com.br.duplication.server.netty.ResponseSender;

@Sharable
public class DataRequestHandler implements NettyHttpRequestHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DataRequestHandler.class);
	
	private Map<HttpMethod, MessageHandler<DataMessage>> methodToOps = new HashMap<HttpMethod, MessageHandler<DataMessage>>();
	
	public void addMessageHandler(String method, MessageHandler<DataMessage> handler) {
		methodToOps.put(HttpMethod.valueOf(method), handler);
	}

	@Override
	public void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request) {
		LOG.debug("handle request[{}:{}]", request.method(), request.uri());
		
		MessageHandler<DataMessage> handler = methodToOps.get(request.method());
		if(handler == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
			return;
		}
		
		QueryStringDecoder decoder = new QueryStringDecoder(request.uri(), CharsetUtil.UTF_8, true);
		
		DataMessage message = new DataMessage();
		
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
