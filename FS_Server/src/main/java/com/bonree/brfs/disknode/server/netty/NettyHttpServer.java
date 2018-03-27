package com.bonree.brfs.disknode.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

import com.bonree.brfs.disknode.utils.LifeCycle;

public class NettyHttpServer implements LifeCycle {
	private int conTimeout = 50000;//连接超时时间(毫秒)
	private int baklog = 10000;//积压请求数
	
	private String ip;
	private int port;
	private ChannelFuture channelFuture;
	
	private NettyChannelInitializer handlerInitializer;
	
	private EventLoopGroup bossGroup = new NioEventLoopGroup(2);
	private EventLoopGroup workerGroup = new NioEventLoopGroup(6);
	
	public NettyHttpServer(int port) {
		this(null, port);
	}
	
	public NettyHttpServer(String ip, int port) {
		this.ip = ip;
		this.port = port;
		this.handlerInitializer = new NettyChannelInitializer();
	}
	
	@Override
	public void start() throws InterruptedException {
		ServerBootstrap serverStart = new ServerBootstrap();
		serverStart.group(bossGroup, workerGroup);
		serverStart.channel(NioServerSocketChannel.class);
		serverStart.childHandler(handlerInitializer);
		serverStart.option(ChannelOption.SO_BACKLOG, baklog);//积压数量
		serverStart.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,conTimeout);//连接超时时间(毫秒)
		serverStart.childOption(ChannelOption.SO_KEEPALIVE, true);//保持连接
		
		InetSocketAddress address = (ip == null ? new InetSocketAddress(port) : new InetSocketAddress(ip, port));
		channelFuture = serverStart.bind(address).sync();
	}

	@Override
	public void stop() {
		try {
			channelFuture.channel().close().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}
	
	public void addContextHandler(NettyHttpContextHandler contextHttpHandler) {
		handlerInitializer.addContextHandler(contextHttpHandler);
	}
	
	public void closeServer() {
		if (channelFuture != null) {
			channelFuture.channel().close();
		}
	}
}
