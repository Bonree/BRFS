package com.bonree.brfs.common.net.tcp.client;

import java.util.List;

import com.bonree.brfs.common.net.tcp.BaseResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class BaseResponseDecoder extends ByteToMessageDecoder {
	private BaseResponse response;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		if(response == null) {
			if(in.readableBytes() < Integer.BYTES * 3) {
				return;
			}
			
			int token = in.readInt();
			int code = in.readInt();
			response = new BaseResponse(token, code);
			
			int length = in.readInt();
			byte[] bytes = new byte[length];
			response.setBody(bytes);
			
			if(length == 0) {
				out.add(response);
				response = null;
				return;
			}
		}
		
		if(in.readableBytes() < response.getBody().length) {
			return;
		}
		
		in.readBytes(response.getBody());
		out.add(response);
		response = null;
	}

}
