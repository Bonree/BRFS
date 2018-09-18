package com.bonree.brfs.common.net.tcp.file.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.bonree.brfs.common.net.tcp.TokenMessage;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

public class ReadObjectEncoder extends MessageToByteEncoder<TokenMessage<ReadObject>> {

	@Override
	protected void encode(ChannelHandlerContext ctx, TokenMessage<ReadObject> object, ByteBuf out)
			throws Exception {
		ReadObject readObject = object.message();
		
		readObject.setToken(object.messageToken());
//		out.writeBytes(JsonUtils.toJsonBytes(object.message()));
		
		out.writeBytes(Joiner.on(';')
				.join(readObject.getFilePath(),
						readObject.getOffset(),
						readObject.getLength(),
						readObject.getRaw(),
						readObject.getToken(),
						"\n").getBytes(Charsets.UTF_8));
	}

}
