package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;

/**
 * Netty实现的Http请求处理接口
 * 
 * @author chen
 *
 */
public class NettyHttpRequestHandler {
	private static final Logger LOG = LoggerFactory.getLogger(NettyHttpRequestHandler.class);
	private Map<HttpMethod, MessageHandler> methodToOps = new HashMap<HttpMethod, MessageHandler>();

	public void addMessageHandler(String method, MessageHandler handler) {
		methodToOps.put(HttpMethod.valueOf(method), handler);
	}

	public void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request) {
		LOG.debug("handle request[{}:{}]", request.method(), request.uri());
		
		MessageHandler handler = methodToOps.get(request.method());
		if(handler == null) {
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, HttpResponseStatus.METHOD_NOT_ALLOWED.reasonPhrase());
			return;
		}
		
		QueryStringDecoder decoder = new QueryStringDecoder(request.uri(), CharsetUtil.UTF_8, true);
		
		HttpMessage message = new HttpMessage();
		message.setPath(decoder.path());
		message.setParams(HttpParamsDecoder.decode(request));
		
		byte[] data = new byte[request.content().readableBytes()];
		request.content().readBytes(data);
		message.setContent(data);
		
		try {
			if(!handler.isValidRequest(message)) {
				ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.reasonPhrase());
				return;
			}
			
			handler.handle(message, new DefaultNettyHandleResultCallback(ctx));
		} catch (Exception e) {
			e.printStackTrace();
			ResponseSender.sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toString());
		}
	}
}
