package com.bonree.brfs.common.net.tcp.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.TokenMessage;

public class BaseMessageEncoder extends MessageToByteEncoder<TokenMessage<BaseMessage>> {

	@Override
	protected void encode(ChannelHandlerContext ctx, TokenMessage<BaseMessage> msg, ByteBuf out)
			throws Exception {
		out.writeByte((byte) 0xBF);
		out.writeInt(msg.messageToken());
		
		BaseMessage baseMessage = msg.message();
		out.writeByte(baseMessage.getType());
		
		int length = baseMessage.getBody() == null ? 0 : baseMessage.getBody().length;
		out.writeInt(length);
		
		if(length > 0) {
			out.writeBytes(baseMessage.getBody());
		}
	}

}
