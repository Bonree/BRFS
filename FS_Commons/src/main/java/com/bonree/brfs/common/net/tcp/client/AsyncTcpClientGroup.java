package com.bonree.brfs.common.net.tcp.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.utils.PooledThreadFactory;

public class AsyncTcpClientGroup implements TcpClientGroup<BaseMessage, BaseResponse, TcpClientConfig>, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncTcpClientGroup.class);
	
	private EventLoopGroup group;
	
	private static final int DEFAULT_WRITE_IDLE_TIMEOUT_SECONDS = 5;

	public AsyncTcpClientGroup(int workerNum) {
		this.group = new NioEventLoopGroup(workerNum, new PooledThreadFactory("async_client"));
	}

	@Override
	public TcpClient<BaseMessage, BaseResponse> createClient(TcpClientConfig config, Executor executor)
			throws InterruptedException {
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeoutMillis());
		
		AsyncTcpClient client = new AsyncTcpClient(executor);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new IdleStateHandler(0, DEFAULT_WRITE_IDLE_TIMEOUT_SECONDS, 0))
				             .addLast(new BaseMessageEncoder())
				             .addLast(new BaseResponseDecoder())
				             .addLast(new SimpleChannelInboundHandler<BaseResponse>() {

								@Override
								protected void channelRead0(
										ChannelHandlerContext ctx,
										BaseResponse msg) throws Exception {
									client.handleResponse(msg);
								}

								@Override
								public void userEventTriggered(
										ChannelHandlerContext ctx, Object evt)
										throws Exception {
										if (evt instanceof IdleStateEvent) {
											IdleStateEvent e = (IdleStateEvent) evt;
											if (e.state() == IdleState.WRITER_IDLE) {
												ctx.writeAndFlush(new BaseMessage(-1));
											}
										}
								}
								
								
							});
			}
			
		});
		
		ChannelFuture future = bootstrap.connect(config.remoteAddress()).sync();
		if(!future.isSuccess()) {
			return null;
		}
		
		LOG.info("create tcp client for {}", config.remoteAddress());
		client.attach(future.channel());
		return client;
	}

	@Override
	public void close() throws IOException {
		try {
			group.shutdownGracefully().sync();
		} catch (InterruptedException e) {}
	}
}
