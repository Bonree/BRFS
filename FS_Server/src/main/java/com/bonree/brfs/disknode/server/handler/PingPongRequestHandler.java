package com.bonree.brfs.disknode.server.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import io.netty.channel.ChannelHandler.Sharable;

@Sharable
public class PingPongRequestHandler implements MessageHandler {

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        callback.completed(new HandleResult(true));
    }

}
