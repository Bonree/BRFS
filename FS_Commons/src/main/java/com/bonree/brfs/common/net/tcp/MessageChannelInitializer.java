package com.bonree.brfs.common.net.tcp;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.util.concurrent.Executor;
import jdk.internal.dynalink.beans.StaticClass;

public class MessageChannelInitializer extends ChannelInitializer<SocketChannel> {
    private MessageDispatcher messageDispatcher;

    private static final int DEFAULT_READ_IDLE_TIMEOUT_SECONDS = 30;
    private static final int LIMIT = Configs.getConfiguration().getConfig(DataNodeConfigs.TRAFFIC_LIMIT);

    public MessageChannelInitializer(Executor executor) {
        this.messageDispatcher = new MessageDispatcher(executor);
    }

    public void addMessageHandler(int type, MessageHandler<BaseResponse> handler) {
        messageDispatcher.addHandler(type, handler);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        ChannelTrafficShapingHandler channelTrafficShapingHandler = new ChannelTrafficShapingHandler(LIMIT, LIMIT);
        pipeline.addLast(channelTrafficShapingHandler);
        pipeline.addLast(new MessageResponseEncoder());
        pipeline.addLast(new MessageProtocolDecoder());
        pipeline.addLast(messageDispatcher);
    }

}
