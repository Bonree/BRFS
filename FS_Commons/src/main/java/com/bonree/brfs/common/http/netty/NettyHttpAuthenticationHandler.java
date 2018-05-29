package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

public class NettyHttpAuthenticationHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private HttpAuthenticator httpAuthenticator;
	
	public NettyHttpAuthenticationHandler(HttpAuthenticator httpAuthenticator) {
		super(false);
		this.httpAuthenticator = httpAuthenticator;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
			throws Exception {
		HttpHeaders headers = request.headers();
		String userName = headers.get("username");
		String passwd = headers.get("password");
		
		if(!httpAuthenticator.isLegal(userName, passwd)) {
			ReferenceCountUtil.release(request);
			ResponseSender.sendError(ctx, HttpResponseStatus.FORBIDDEN, HttpResponseStatus.FORBIDDEN.reasonPhrase());
			return;
		}
		
		ctx.fireChannelRead(request);
	}

}
