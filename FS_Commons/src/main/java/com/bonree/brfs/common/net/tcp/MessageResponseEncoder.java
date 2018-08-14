package com.bonree.brfs.common.net.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageResponseEncoder extends MessageToByteEncoder<BaseResponse>{

	@Override
	protected void encode(ChannelHandlerContext ctx, BaseResponse msg, ByteBuf out) throws Exception {
		out.writeInt(msg.getToken());
		out.writeInt(msg.getCode());
		
		int length = msg.getBody() == null ? 0 : msg.getBody().length;
		out.writeInt(length);
		
		if(length > 0) {
			out.writeBytes(msg.getBody());
		}
	}

}
