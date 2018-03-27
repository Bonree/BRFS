package com.br.disknode.server.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

public interface NettyHttpRequestHandler {
	void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request);
}
