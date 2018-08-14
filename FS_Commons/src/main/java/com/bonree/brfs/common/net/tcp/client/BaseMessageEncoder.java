package com.bonree.brfs.common.net.tcp.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.bonree.brfs.common.net.tcp.BaseMessage;

public class BaseMessageEncoder extends MessageToByteEncoder<BaseMessage> {

	@Override
	protected void encode(ChannelHandlerContext ctx, BaseMessage msg, ByteBuf out)
			throws Exception {
		out.writeByte((byte) 0xBF);
		out.writeInt(msg.getToken());
		out.writeByte(msg.getType());
		
		int length = msg.getBody() == null ? 0 : msg.getBody().length;
		out.writeInt(length);
		
		if(length > 0) {
			out.writeBytes(msg.getBody());
		}
	}

}
