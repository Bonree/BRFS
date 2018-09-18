package com.bonree.brfs.common.net.tcp.file;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;

public class ReadObjectStringDecoder extends ByteToMessageDecoder {

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		List<String> parts = Splitter.on(';').splitToList(in.toString(Charsets.UTF_8));
		in.skipBytes(in.readableBytes());
		
		ReadObject object = new ReadObject();
		object.setFilePath(parts.get(0));
		object.setOffset(Long.parseLong(parts.get(1)));
		object.setLength(Integer.parseInt(parts.get(2)));
		object.setRaw(Integer.parseInt(parts.get(3)));
		object.setToken(Integer.parseInt(parts.get(4)));
		
		out.add(object);
	}

}
