package com.bonree.brfs.common.net.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageResponseEncoder extends MessageToByteEncoder<TokenMessage<BaseResponse>>{
	private static final Logger LOG = LoggerFactory.getLogger(MessageResponseEncoder.class);

	@Override
	protected void encode(ChannelHandlerContext ctx, TokenMessage<BaseResponse> msg, ByteBuf out) throws Exception {
		LOG.info("encoding response[{}, {}]", msg.messageToken());
		out.writeInt(msg.messageToken());
		
		BaseResponse response = msg.message();
		out.writeInt(response.getCode());
		
		int length = response.getBody() == null ? 0 : response.getBody().length;
		out.writeInt(length);
		
		if(length > 0) {
			out.writeBytes(response.getBody());
		}
	}

}
