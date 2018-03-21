package com.br.duplication.server.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.ArrayList;
import java.util.List;

public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {
	private List<NettyHttpContextHandler> contextHandlers = new ArrayList<NettyHttpContextHandler>();

	public void addContextHandler(NettyHttpContextHandler handler) {
		contextHandlers.add(handler);
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
        // server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
		pipeline.addLast(new HttpResponseEncoder());
        // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
		pipeline.addLast(new HttpRequestDecoder());
		pipeline.addLast(new HttpObjectAggregator(65536));
		pipeline.addLast(new ChunkedWriteHandler());
		contextHandlers.forEach((NettyHttpContextHandler handler) -> pipeline.addLast(handler));
    }

}
