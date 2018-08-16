package com.bonree.brfs.common.net.tcp.file.client;

import com.bonree.brfs.common.net.tcp.TokenMessage;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.utils.JsonUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ReadObjectEncoder extends MessageToByteEncoder<TokenMessage<ReadObject>> {

	@Override
	protected void encode(ChannelHandlerContext ctx, TokenMessage<ReadObject> object, ByteBuf out)
			throws Exception {
		object.message().setToken(object.messageToken());
		out.writeBytes(JsonUtils.toJsonBytes(object.message()));
	}

}
