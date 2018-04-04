package com.bonree.brfs.common.http.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.google.common.primitives.Bytes;

public class DefaultNettyHandleResultCallback implements HandleResultCallback {
	private ChannelHandlerContext context;
	
	public DefaultNettyHandleResultCallback(ChannelHandlerContext ctx) {
		this.context = ctx;
	}

	@Override
	public void completed(HandleResult result) {
		HttpResponseStatus status = result.isSuccess() ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
		
		byte[] errorBytes = result.getCause() != null ? BrStringUtils.toUtf8Bytes(result.getCause().toString()) : new byte[0];
		byte[] dataBytes = result.getData() != null ? result.getData() : new byte[0];
		
		ByteBuf content = Unpooled.wrappedBuffer(Bytes.concat(errorBytes, dataBytes));
		
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        ResponseSender.sendResponse(context, response);
	}
	
}
