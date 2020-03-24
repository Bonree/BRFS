package com.bonree.brfs.duplication.rocksdb.tmp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class RocksDBTest {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBTest.class);

    public void startServer(final ChannelHandler... handlers) {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(2);

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new HttpRequestDecoder());
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
                    pipeline.addLast(new ChunkedWriteHandler());
                    pipeline.addLast(handlers);
                    pipeline.addLast(new HttpResponseEncoder());
                }
            });
            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, false);  // 保持长连接
            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture f = serverBootstrap.bind(12020).sync();
            LOG.info(String.format("Http server startup successful! ip:[%s], port:[%s]", InetAddress.getLocalHost().getHostAddress(), 12020));
            f.channel().closeFuture().sync();
        } catch (Exception ex) {
            LOG.error("Http sever startup error!", ex);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
