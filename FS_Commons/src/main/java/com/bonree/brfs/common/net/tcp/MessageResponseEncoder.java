package com.bonree.brfs.common.net.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageResponseEncoder extends MessageToByteEncoder<BaseResponse>{
	private static final Logger LOG = LoggerFactory.getLogger(MessageResponseEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, BaseResponse msg, ByteBuf out) throws Exception {
		LOG.info("encoding response[{}, {}]", msg.getToken());
		out.writeInt(msg.getToken());
		out.writeInt(msg.getCode());
		
		int length = msg.getBody() == null ? 0 : msg.getBody().length;
		out.writeInt(length);
		
		if(length > 0) {
			out.writeBytes(msg.getBody());
		}
	}

}
