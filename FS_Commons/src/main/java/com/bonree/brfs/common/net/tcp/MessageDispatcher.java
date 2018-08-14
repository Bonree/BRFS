package com.bonree.brfs.common.net.tcp;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MessageDispatcher extends SimpleChannelInboundHandler<BaseMessage>{
	private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);
	
	private Map<Integer, MessageHandler> handlers = new HashMap<Integer, MessageHandler>();
	
	public void addHandler(int type, MessageHandler handler) {
		handlers.put(type, handler);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, BaseMessage msg) throws Exception {
		MessageHandler handler = handlers.get(msg.getType());
		if(handler == null) {
			LOG.error("unknown type[{}] of message!", msg.getType());
			ctx.writeAndFlush(new BaseResponse(msg.getToken(), ResponseCode.ERROR));
			return;
		}
		
		try {
			handler.handleMessage(msg, new HandleCallback() {
				
				@Override
				public void complete(BaseResponse response) {
					ctx.writeAndFlush(response);
				}
			});
		} catch (Exception e) {
			LOG.error("handle message error", e);
			
			ctx.writeAndFlush(new BaseResponse(msg.getToken(), ResponseCode.ERROR));
		}
		
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
}
