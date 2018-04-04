package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

import com.bonree.brfs.common.http.MessageHandler;

/**
 * Netty实现的Http请求处理接口
 * 
 * @author chen
 *
 */
public interface NettyHttpRequestHandler<T> {
	void addMessageHandler(String method, MessageHandler<T> handler);
	void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request);
}
