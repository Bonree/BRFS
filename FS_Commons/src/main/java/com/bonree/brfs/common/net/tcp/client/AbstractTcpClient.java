package com.bonree.brfs.common.net.tcp.client;

import com.bonree.brfs.common.net.tcp.TokenMessage;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTcpClient<S, R> implements TcpClient<S, R> {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncTcpClient.class);

    private Channel channel;

    protected Executor executor;
    private final AtomicInteger tokenMaker = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, ResponseHandler<R>> handlers;

    private TcpClientCloseListener listener;

    protected AbstractTcpClient(Executor executor) {
        this.executor = executor;
        this.handlers = new ConcurrentHashMap<>();
    }

    public void attach(Channel channel) {
        Preconditions.checkNotNull(channel);
        this.channel = channel;
        this.channel.closeFuture().addListener(new ChannelCloseListener());
    }

    @Override
    public String remoteHost() {
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        return address.getHostString();
    }

    @Override
    public int remotePort() {
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        return address.getPort();
    }

    @Override
    public void sendMessage(S msg, ResponseHandler<R> handler) {
        try {
            Preconditions.checkNotNull(msg);
            Preconditions.checkNotNull(handler);

            final int token = tokenMaker.getAndIncrement() & Integer.MAX_VALUE;
            LOG.debug("send message with token [{}]", token);
            handlers.put(token, handler);
            channel.writeAndFlush(new TokenMessage<>(token, msg))
                   .addListener((ChannelFutureListener) future -> {
                       if (!future.isSuccess()) {
                           handlers.remove(token);
                           executor.execute(() -> handler.error(new Exception("send message of token[" + token + "] error")));

                           channel.close();
                       }
                   });
        } catch (Throwable e) {
            LOG.error("error when send message [{}]", msg);
            handler.error(e);
        }
    }

    protected ResponseHandler<R> takeHandler(int token) {
        return handlers.remove(token);
    }

    protected abstract void handle(int token, R response);

    @Override
    public void close() throws IOException {
        this.channel.close();
    }

    @Override
    public void setClientCloseListener(TcpClientCloseListener listener) {
        this.listener = listener;
    }

    private class ChannelCloseListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) {
            LOG.warn("channel closed!");
            if (listener != null) {
                executor.execute(() -> {
                    try {
                        listener.clientClosed();
                    } catch (Exception e) {
                        LOG.error("call tcp client close listener error", e);
                    }
                });
            }

            for (ResponseHandler<R> handler : handlers.values()) {
                executor.execute(() -> handler.error(new Exception("channel is closed!")));
            }
        }

    }
}
