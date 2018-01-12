package com.bonree.brfs.nettyhttp.server;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyMessageServer {

	public void bind(int port) throws Exception {
		// 配置服务端的NIO线程组
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 100)
					.handler(new LoggingHandler(LogLevel.INFO)).childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) {
							ch.pipeline().addLast("http-decoder", new HttpRequestDecoder()); 
							ch.pipeline().addLast("http-servercodec",new HttpServerCodec());  
							ch.pipeline().addLast("http-aggegator",new HttpObjectAggregator(1024*1024*64));//定义缓冲数据量  
//							ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
//							ch.pipeline().addLast(
//									new ProtobufDecoder(NettyMessageProto.NettyMessageReqRes.getDefaultInstance()));
//							ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
//							ch.pipeline().addLast(new ProtobufEncoder());
//							ch.pipeline().addLast("http-chunked",new ChunkedWriteHandler());
							ch.pipeline().addLast(new NettyMessageServerHandler());
							ch.pipeline().addLast("http-responseencoder",new HttpResponseEncoder());
						}
					});

			// 绑定端口，同步等待成功
			ChannelFuture f = b.bind(port).sync();

			System.out.println("init start");
			// 等待服务端监听端口关闭
			f.channel().closeFuture().sync();
		} finally {
			// 优雅退出，释放线程池资源
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
		int port = 8080;
		new NettyMessageServer().bind(port);
	}

}
