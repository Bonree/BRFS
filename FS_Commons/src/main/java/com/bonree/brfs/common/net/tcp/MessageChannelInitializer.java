package com.bonree.brfs.common.net.tcp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import java.util.concurrent.Executor;

public class MessageChannelInitializer extends ChannelInitializer<SocketChannel> {
    private MessageDispatcher messageDispatcher;

    private static final int DEFAULT_READ_IDLE_TIMEOUT_SECONDS = 30;

    public MessageChannelInitializer(Executor executor) {
        this.messageDispatcher = new MessageDispatcher(executor);
    }

    public void addMessageHandler(int type, MessageHandler<BaseResponse> handler) {
        messageDispatcher.addHandler(type, handler);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new MessageResponseEncoder());
        pipeline.addLast(new MessageProtocolDecoder());
        pipeline.addLast(messageDispatcher);
    }

}
