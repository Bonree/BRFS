package com.bonree.brfs.disknode.server.netty;

import io.netty.channel.ChannelHandler.Sharable;
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

import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.http.netty.DefaultNettyHandleResultCallback;
import com.bonree.brfs.common.http.netty.HttpParamsDecoder;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.ResponseSender;
import com.bonree.brfs.disknode.server.DiskMessage;

@Sharable
public class DiskNettyHttpRequestHandler implements NettyHttpRequestHandler<DiskMessage> {
	private static final Logger LOG = LoggerFactory.getLogger(DiskNettyHttpRequestHandler.class);
	
	private Map<HttpMethod, MessageHandler<DiskMessage>> methodToOps = new HashMap<HttpMethod, MessageHandler<DiskMessage>>();
	
	@Override
	public void addMessageHandler(String method, MessageHandler<DiskMessage> handler) {
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
		
		message.setParams(HttpParamsDecoder.decode(request));
		
		byte[] data = new byte[request.content().readableBytes()];
		request.content().readBytes(data);
		message.setData(data);
		
		handler.handle(message, new DefaultNettyHandleResultCallback(ctx));
	}
}
