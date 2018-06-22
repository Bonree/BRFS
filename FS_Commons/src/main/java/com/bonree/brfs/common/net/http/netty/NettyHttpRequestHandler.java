package com.bonree.brfs.common.net.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.timer.TimeCounter;

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
			LOG.error("Exception context[{}] method[{}] is unknown", ctx.toString(), request.method());
			ResponseSender.sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, HttpResponseStatus.METHOD_NOT_ALLOWED.reasonPhrase());
			return;
		}
		
		QueryStringDecoder decoder = new QueryStringDecoder(request.uri(), CharsetUtil.UTF_8, true);
		
		HttpMessage message = new HttpMessage() {
			private byte[] content;

			@Override
			public String getPath() {
				return decoder.path();
			}

			@Override
			public Map<String, String> getParams() {
				return HttpParamsDecoder.decode(request);
			}

			@Override
			public byte[] getContent() {
				if(content == null) {
					content = new byte[request.content().readableBytes()];
					request.content().readBytes(content);
				}
				
				return content;
			}
			
		};
		
		try {
			if(!handler.isValidRequest(message)) {
				LOG.error("Exception context[{}] method[{}] invalid request message[{}]", ctx.toString(), message.getPath());
				ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.reasonPhrase());
				return;
			}
			
			handler.handle(message, new DefaultNettyHandleResultCallback(ctx));
		} catch (Exception e) {
			LOG.error("message handle error", e);
			ResponseSender.sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toString());
		}
	}
}
