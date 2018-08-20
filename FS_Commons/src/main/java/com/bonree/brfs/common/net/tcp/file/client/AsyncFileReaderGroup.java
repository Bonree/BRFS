package com.bonree.brfs.common.net.tcp.file.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientGroup;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.utils.PooledThreadFactory;

public class AsyncFileReaderGroup implements TcpClientGroup<ReadObject, FileContentPart, AsyncFileReaderCreateConfig>, Closeable {
	private EventLoopGroup group;
	
	public AsyncFileReaderGroup(int workerNum) {
		this.group = new NioEventLoopGroup(workerNum, new PooledThreadFactory("async_file_reader"));
	}
	
	@Override
	public TcpClient<ReadObject, FileContentPart> createClient(AsyncFileReaderCreateConfig config) throws InterruptedException {
		return createClient(config, null);
	}

	@Override
	public TcpClient<ReadObject, FileContentPart> createClient(AsyncFileReaderCreateConfig config, Executor executor) throws InterruptedException {
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeoutMillis());
		
		if(executor == null) {
			executor = new Executor() {
				
				@Override
				public void execute(Runnable command) {
					command.run();
				}
			};
		}
		FileReadClient reader = new FileReadClient(executor);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ReadObjectEncoder())
						.addLast(new ByteToMessageDecoder() {
							private int token;
							private int readingLength = 0;

							@Override
							protected void decode(ChannelHandlerContext ctx,
									ByteBuf in, List<Object> out)
									throws Exception {
								if (readingLength == 0) {
									if (in.readableBytes() < Integer.BYTES * 2) {
										return;
									}

									token = in.readInt();
									readingLength = in.readInt();
									
									if (readingLength < 0) {
										reader.handle(token, new FileContentPart(null, true));

										readingLength = 0;
										return;
									}

									if (readingLength == 0) {
										reader.handle(token, new FileContentPart(new byte[0], true));
										return;
									}
								}

								int readableLength = Math.min(readingLength, in.readableBytes());
								if (readableLength == 0) {
									return;
								}

								byte[] bytes = new byte[readableLength];
								in.readBytes(bytes);
								readingLength -= readableLength;
								
								reader.handle(token, new FileContentPart(bytes, readingLength == 0));
							}
						});
			}
			
		});
		
		ChannelFuture future = bootstrap.connect(config.remoteAddress()).sync();
		if(!future.isSuccess()) {
			return null;
		}
		
		reader.attach(future.channel());
		return reader;
	}

	@Override
	public void close() throws IOException {
		try {
			group.shutdownGracefully().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
