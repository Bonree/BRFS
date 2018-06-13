package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * Netty的Handler初始化类
 * 
 * @author chen
 *
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {
	private NettyHttpContextHandler contextHandler = new NettyHttpContextHandler();
	
	private NettyHttpAuthenticationHandler authenticationHandler;
	private final int maxHttpContentLength;
	
	NettyChannelInitializer(int maxHttpContentLength) {
		this.maxHttpContentLength = maxHttpContentLength;
	}
	
	public void addAuthenticationHandler(NettyHttpAuthenticationHandler authenticationHandler) {
		this.authenticationHandler = authenticationHandler;
	}
	
	public void addRequestHandler(String root, NettyHttpRequestHandler handler) {
		contextHandler.add(root, handler);
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
        // server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
		pipeline.addLast(new HttpResponseEncoder());
        // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
		pipeline.addLast(new HttpRequestDecoder());
		pipeline.addLast(new HttpObjectAggregator(maxHttpContentLength));
		pipeline.addLast(new ChunkedWriteHandler());
		
		if(authenticationHandler != null) {
			pipeline.addLast(authenticationHandler);
		}
		
		pipeline.addLast(contextHandler);	
    }
}
