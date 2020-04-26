package com.bonree.brfs.disknode.server.tcp.handler;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;

public class PingPongMessageHandler implements MessageHandler<BaseResponse> {

    @Override
    public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
        writer.write(new BaseResponse(ResponseCode.OK));
    }

}
