package com.bonree.brfs.common.net.tcp;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MessageDispatcher extends SimpleChannelInboundHandler<TokenMessage<BaseMessage>> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

    private final Executor executor;
    private final Map<Integer, MessageHandler<BaseResponse>> handlers = new HashMap<>();

    public MessageDispatcher(Executor executor) {
        this.executor = executor;
    }

    public void addHandler(int type, MessageHandler<BaseResponse> handler) {
        handlers.put(type, handler);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TokenMessage<BaseMessage> msg) {
        BaseMessage baseMessage = msg.message();
        int token = msg.messageToken();
        MessageHandler<BaseResponse> handler = handlers.get(baseMessage.getType());
        if (handler == null) {
            LOG.error("unknown type[{}] of message!", baseMessage.getType());
            ctx.writeAndFlush(new TokenMessage<>(token, new BaseResponse(ResponseCode.ERROR_PROTOCOL)));
            return;
        }

        LOG.debug("handle base message[{}, {}]", token, baseMessage.getType());

        try {
            executor.execute(() -> {
                try {
                    handler.handleMessage(baseMessage, response ->
                        ctx.writeAndFlush(new TokenMessage<BaseResponse>(token, response)));
                } catch (Throwable e) {
                    LOG.error("handle message error", e);
                    ctx.writeAndFlush(new TokenMessage<>(token,
                                                         new BaseResponse(ResponseCode.ERROR)));
                }
            });
        } catch (RejectedExecutionException rejectedExecutionException) {
            ctx.writeAndFlush(new TokenMessage<>(token,
                                                 new BaseResponse(ResponseCode.ERROR)));
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("handle message error :[{}]", cause.getMessage());
        super.exceptionCaught(ctx, cause);
    }
}
