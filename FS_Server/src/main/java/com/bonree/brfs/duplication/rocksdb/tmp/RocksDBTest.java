package com.bonree.brfs.duplication.rocksdb.tmp;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RocksDBTest implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBTest.class);
    private ExecutorService executorService;
    private ChannelHandler handler;

    public RocksDBTest(ChannelHandler handler) {
        this.handler = handler;
    }

    @Override
    public void start() throws Exception {
        executorService = Executors.newSingleThreadExecutor(new PooledThreadFactory("tmp_netty_server"));
        startServer(this.handler);
    }

    @Override
    public void stop() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
    }


    public void startServer(ChannelHandler handler) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
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
                            pipeline.addLast(handler);
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
        });

    }

}
