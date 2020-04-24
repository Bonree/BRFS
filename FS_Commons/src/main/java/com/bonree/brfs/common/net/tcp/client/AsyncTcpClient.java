package com.bonree.brfs.common.net.tcp.client;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTcpClient extends AbstractTcpClient<BaseMessage, BaseResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncTcpClient.class);

    AsyncTcpClient(Executor executor) {
        super(executor);
    }

    @Override
    protected void handle(int token, BaseResponse response) {
        ResponseHandler<BaseResponse> handler = takeHandler(token);
        if (handler == null) {
            LOG.error("no handler is found for response[{}]", token);
            return;
        }

        handler.handle(response);
    }
}
