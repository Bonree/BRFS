package com.bonree.brfs.common.net.tcp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class MessageChannelInitializer extends ChannelInitializer<SocketChannel> {
	private MessageDispatcher messageDispatcher = new MessageDispatcher();
	
	private static final int DEFAULT_READ_IDLE_TIMEOUT_SECONDS = 10;
	
	public void addMessageHandler(int type, MessageHandler handler) {
		messageDispatcher.addHandler(type, handler);
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast(new IdleStateHandler(DEFAULT_READ_IDLE_TIMEOUT_SECONDS, 0, 0));
		pipeline.addLast(new MessageResponseEncoder());
		pipeline.addLast(new MessageProtocolDecoder());
		pipeline.addLast(messageDispatcher);
	}

}
