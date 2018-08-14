package com.bonree.brfs.common.net.tcp.file.client;

import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.utils.JsonUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ReadObjectEncoder extends MessageToByteEncoder<ReadObject> {

	@Override
	protected void encode(ChannelHandlerContext ctx, ReadObject object, ByteBuf out)
			throws Exception {
		out.writeBytes(JsonUtils.toJsonBytes(object));
	}

}
