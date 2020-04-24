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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.URI;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.glassfish.jersey.server.ResourceConfig;

public class NettyHttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private final int maxContentLength;

    private final URI baseUri;
    private final NettyHttpContainer container;
    private final ResourceConfig resourceConfig;

    @Inject
    public NettyHttpServerInitializer(
        @Nullable SslContext sslCtx,
        NettyHttpServerConfig httpConfig,
        @RootUri URI uri,
        NettyHttpContainer container,
        ResourceConfig resourceConfig) {
        this.sslCtx = sslCtx;
        this.maxContentLength = httpConfig.getMaxHttpContentLength();
        this.baseUri = requireNonNull(uri, "uri is null");
        this.container = requireNonNull(container, "container is null");
        this.resourceConfig = requireNonNull(resourceConfig, "resourceConfig is null");
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(maxContentLength));
        p.addLast(new ChunkedWriteHandler());
        p.addLast(new JerseyServerHandler(baseUri, container, resourceConfig));
    }

}
