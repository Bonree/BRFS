package com.bonree.brfs.common.net.tcp.file;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileChannelInitializer extends ChannelInitializer<SocketChannel> {
	private SimpleFileReadHandler fileReadHandler;
	
	public FileChannelInitializer(ReadObjectTranslator translator) {
		this.fileReadHandler = new SimpleFileReadHandler(translator);
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
//		pipeline.addLast(new JsonBytesDecoder(true));
//		pipeline.addLast(new ReadObjectDecoder());
		pipeline.addLast(new LineBasedFrameDecoder(1024 * 16));
		pipeline.addLast(new ReadObjectStringDecoder());
		pipeline.addLast(new ChunkedWriteHandler());
		pipeline.addLast(fileReadHandler);
	}

}
