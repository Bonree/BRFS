package com.bonree.brfs.common.http.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;

import com.google.common.base.Splitter;

@Sharable
public class NettyHttpContextHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private String contextPath;
	private List<String> uriSegments;
	private NettyHttpRequestHandler<?> requestHandler;
	
	public NettyHttpContextHandler(String uriRoot) {
		this(uriRoot, null);
	}

	public NettyHttpContextHandler(String uriRoot, NettyHttpRequestHandler<?> handler) {
		this.contextPath = uriRoot;
		this.uriSegments = Splitter.on('/').trimResults().omitEmptyStrings().splitToList(uriRoot);
		this.requestHandler = handler;
	}
	
	public String getContextPath() {
		return contextPath;
	}
	
	public void setNettyHttpRequestHandler(NettyHttpRequestHandler<?> handler) {
		this.requestHandler = handler;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx,
			FullHttpRequest request) throws Exception {
		if(!request.decoderResult().isSuccess()) {
			//请求解析失败
			ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST);
			return;
		}
		
		if (!isValidUri(request.uri())) {
			//请求URI无效
			ctx.fireChannelRead(request);
			return;
		}

		//删除context path，后续的handler可以更便捷的处理uri
		request.setUri(removeContextPath(request.uri()));
		requestHandler.requestReceived(ctx, request);
	}
	
	private String removeContextPath(String uri) {
		return uri.substring(contextPath.length());
	}

	private boolean isValidUri(String uri) {
		List<String> paths = Splitter.on('/').trimResults().omitEmptyStrings().splitToList(uri);
		if(paths.size() < uriSegments.size()) {
			return false;
		}
		
		for(int i = 0; i < uriSegments.size(); i++) {
			if(!uriSegments.get(i).equals(paths.get(i))) {
				return false;
			}
		}
		
		return true;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		if (ctx.channel().isActive()) {
			ResponseSender.sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
	}
}
