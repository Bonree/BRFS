package com.bonree.brfs.common.net.tcp;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MessageDispatcher extends SimpleChannelInboundHandler<TokenMessage<BaseMessage>> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

    private Executor executor;
    private Map<Integer, MessageHandler<BaseResponse>> handlers = new HashMap<Integer, MessageHandler<BaseResponse>>();

    public MessageDispatcher(Executor executor) {
        this.executor = executor;
    }

    public void addHandler(int type, MessageHandler<BaseResponse> handler) {
        handlers.put(type, handler);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TokenMessage<BaseMessage> msg) throws Exception {
        BaseMessage baseMessage = msg.message();
        MessageHandler<BaseResponse> handler = handlers.get(baseMessage.getType());
        if (handler == null) {
            LOG.error("unknown type[{}] of message!", baseMessage.getType());
            ctx.writeAndFlush(new TokenMessage<BaseResponse>() {

                @Override
                public int messageToken() {
                    return msg.messageToken();
                }

                @Override
                public BaseResponse message() {
                    return new BaseResponse(ResponseCode.ERROR_PROTOCOL);
                }
            });
            return;
        }

        LOG.info("handle base message[{}, {}]", msg.messageToken(), baseMessage.getType());

        executor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    handler.handleMessage(baseMessage, new ResponseWriter<BaseResponse>() {

                        @Override
                        public void write(BaseResponse response) {
                            ctx.writeAndFlush(new TokenMessage<BaseResponse>() {

                                @Override
                                public int messageToken() {
                                    return msg.messageToken();
                                }

                                @Override
                                public BaseResponse message() {
                                    return response;
                                }
                            });
                        }

                    });
                } catch (Throwable e) {
                    LOG.error("handle message error", e);

                    ctx.writeAndFlush(new TokenMessage<BaseResponse>() {

                        @Override
                        public int messageToken() {
                            return msg.messageToken();
                        }

                        @Override
                        public BaseResponse message() {
                            return new BaseResponse(ResponseCode.ERROR);
                        }
                    });
                }
            }
        });
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
        throws Exception {
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
