package com.bonree.brfs.common.http.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * 基于Netty的Http响应发送类
 * 
 * @author chen
 *
 */
public class ResponseSender {
	
	public static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
				status, Unpooled.copiedBuffer("Failure: " + status.toString()
						+ "\r\n", CharsetUtil.UTF_8));
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
		ctx.writeAndFlush(response);
	}
	
	public static void sendResponse(ChannelHandlerContext ctx, HttpResponse response) {
		ctx.writeAndFlush(response);
	}
}
