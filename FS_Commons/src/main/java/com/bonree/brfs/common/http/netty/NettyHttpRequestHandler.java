package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * Netty实现的Http请求处理接口
 * 
 * @author chen
 *
 */
public interface NettyHttpRequestHandler {
	void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request);
}
