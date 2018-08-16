package com.bonree.brfs.common.net.tcp.file;

import java.util.concurrent.Executor;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileChannelInitializer extends ChannelInitializer<SocketChannel> {
	private FileReadHandler fileReadHandler;
	
	public FileChannelInitializer(ReadObjectTranslator translator, Executor executor) {
		this.fileReadHandler = new FileReadHandler(translator, executor);
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast(new JsonBytesDecoder(true));
		pipeline.addLast(new ReadObjectDecoder());
		pipeline.addLast(new ChunkedWriteHandler());
		pipeline.addLast(fileReadHandler);
	}

}
