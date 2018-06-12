package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

@Sharable
public class NettyHttpContextHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private String contextPath;
	private NettyHttpRequestHandler requestHandler;
	
	public NettyHttpContextHandler(String uriRoot) {
		this(uriRoot, null);
	}

	public NettyHttpContextHandler(String uriRoot, NettyHttpRequestHandler handler) {
		super(false);
		this.contextPath = normalizeRootDir(uriRoot);
		this.requestHandler = handler;
	}
	
	private String normalizeRootDir(String root) {
		Iterable<String> iter = Splitter.on('/').trimResults().omitEmptyStrings().split(root);
		
		StringBuilder builder = new StringBuilder();
		return builder.append('/').append(Joiner.on('/').skipNulls().join(iter)).append('/').toString();
	}
	
	public String getContextPath() {
		return contextPath;
	}
	
	public void setNettyHttpRequestHandler(NettyHttpRequestHandler handler) {
		this.requestHandler = handler;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
		if(!request.decoderResult().isSuccess()) {
			//请求解析失败
			ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.reasonPhrase());
			return;
		}
		
		if (!isValidUri(request.uri())) {
			//请求URI无效
			ctx.fireChannelRead(request);
			return;
		}

		//删除context path，后续的handler可以更便捷的处理uri
		try {
			request.setUri(subUriPath(request.uri()));
			requestHandler.requestReceived(ctx, request);
		} finally {
			ReferenceCountUtil.release(request);
		}
	}
	
	private String subUriPath(String uri) {
		return uri.substring(contextPath.length() - 1);
	}

	private boolean isValidUri(String uri) {
		return uri.startsWith(contextPath);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		if (ctx.channel().isActive()) {
			ResponseSender.sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.toString());
		}
	}
}
