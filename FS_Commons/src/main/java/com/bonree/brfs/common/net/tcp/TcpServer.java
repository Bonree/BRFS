package com.bonree.brfs.common.net.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

import com.bonree.brfs.common.process.LifeCycle;

public class TcpServer implements LifeCycle {
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	
	private ServerConfig config;
	private ChannelInitializer<SocketChannel> channelInitializer;
	
	public TcpServer(ServerConfig config, ChannelInitializer<SocketChannel> channelInitializer) {
		this(config, channelInitializer, new NioEventLoopGroup(config.getBossThreadNums()), new NioEventLoopGroup(config.getWorkerThreadNums()));
	}
	
	public TcpServer(ServerConfig config, ChannelInitializer<SocketChannel> channelInitializer, EventLoopGroup boss, EventLoopGroup worker) {
		this.config = config;
		this.channelInitializer = channelInitializer;
		this.bossGroup = boss;
		this.workerGroup = worker;
	}
	
	@Override
	public void start() throws InterruptedException {
		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.group(bossGroup, workerGroup);
		serverBootstrap.channel(NioServerSocketChannel.class);
		serverBootstrap.option(ChannelOption.SO_BACKLOG, config.getBacklog());//积压数量
		serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);//保持连接
		serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
		serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator());
		serverBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		serverBootstrap.childHandler(channelInitializer);
		
		InetSocketAddress address = (config.getHost() == null ?
				new InetSocketAddress(config.getPort()) : new InetSocketAddress(config.getHost(), config.getPort()));
		serverBootstrap.bind(address).sync();
	}

	@Override
	public void stop() {
		workerGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
	}
}
