package com.bonree.brfs.common.net.http.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class NettyHttpAuthenticationHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private static final Logger LOG = LoggerFactory.getLogger(NettyHttpAuthenticationHandler.class);
	private HttpAuthenticator httpAuthenticator;

	public NettyHttpAuthenticationHandler(HttpAuthenticator httpAuthenticator) {
		super(false);
		this.httpAuthenticator = httpAuthenticator;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
			throws Exception {
		// 内部请求跳过检查
		if(request.uri().startsWith("/rocksdb")){
			ctx.fireChannelRead(request);
			return;
		}

		HttpHeaders headers = request.headers();
		String userName = headers.get("username");
		String passwd = headers.get("password");
		LOG.debug("get user info[{}, {}]", userName, passwd);

		int checkResult = httpAuthenticator.check(userName, passwd);
		if(checkResult != 0) {
			LOG.error("Illegal user info!");
			ReferenceCountUtil.release(request);
			ResponseSender.sendError(ctx, HttpResponseStatus.FORBIDDEN, String.valueOf(checkResult));
			return;
		}

		LOG.debug("OKKK user info!");
		ctx.fireChannelRead(request);
	}

}
