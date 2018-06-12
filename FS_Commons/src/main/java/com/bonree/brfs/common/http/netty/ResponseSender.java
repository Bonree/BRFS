package com.bonree.brfs.common.http.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import com.bonree.brfs.common.utils.BrStringUtils;

/**
 * 基于Netty的Http响应发送类
 * 
 * @author chen
 *
 */
public class ResponseSender {
	
	public static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String reason) {
		byte[] content = reason != null ? BrStringUtils.toUtf8Bytes(reason) : new byte[0];
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(content));
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
		response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);
		sendResponse(ctx, response);
	}
	
	public static void sendResponse(ChannelHandlerContext ctx, HttpResponse response) {
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}
}
