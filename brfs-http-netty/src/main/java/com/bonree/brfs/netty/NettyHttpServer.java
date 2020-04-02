/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonree.brfs.netty;

import static java.util.Objects.requireNonNull;

import java.net.InetSocketAddress;

import javax.inject.Inject;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycleServer;
import com.bonree.brfs.http.HttpServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.SocketUtils;

@ManageLifecycleServer
public class NettyHttpServer implements HttpServer {
    
    private final NettyHttpServerConfig serverConfig;
    private final NettyHttpContainer container;
    private final NettyHttpServerInitializer initializer;
    
    private Channel channel;

    @Inject
    public NettyHttpServer(
            NettyHttpServerConfig config,
            NettyHttpContainer container,
            NettyHttpServerInitializer initializer) {
        this.serverConfig = requireNonNull(config, "config is null");
        this.container = requireNonNull(container, "container is null");
        this.initializer = requireNonNull(initializer, "initializer is null");
    }

    @LifecycleStart
    @Override
    public void start() {
        if(channel != null) {
            throw new IllegalStateException("netty http server has been launched");
        }
        
        final EventLoopGroup bossGroup = new NioEventLoopGroup(serverConfig.getAcceptWorkerNum());
        final EventLoopGroup workerGroup = new NioEventLoopGroup(serverConfig.getRequestHandleWorkerNum());

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, serverConfig.getBacklog())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, serverConfig.getConnectTimeoutMillies())
                .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isKeepAlive())
                .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(initializer);
        
        InetSocketAddress address = new InetSocketAddress(serverConfig.getPort());
        if(serverConfig.getHost() != null) {
            address = SocketUtils.socketAddress(serverConfig.getHost(), serverConfig.getPort());
        }
        
        try {
            channel = bootstrap.bind(address).sync().channel();
            channel.closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {

                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    container.getApplicationHandler().onShutdown(container);
                    
                    bossGroup.shutdownGracefully();
                    workerGroup.shutdownGracefully();
                }
                
            });
        } catch (InterruptedException e) {
            throw new RuntimeException("can not start netty http server", e);
        }
    }

    @LifecycleStop
    @Override
    public void stop() {
        if(channel != null) {
            channel.close();
        }
    }
    
}
