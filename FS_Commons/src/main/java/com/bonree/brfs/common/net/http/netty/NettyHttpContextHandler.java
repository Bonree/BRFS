package com.bonree.brfs.common.net.http.netty;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class NettyHttpContextHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(NettyHttpContextHandler.class);

    private Map<String, NettyHttpRequestHandler> requestHandlers = new TreeMap<String, NettyHttpRequestHandler>();

    public void add(String uriRoot, NettyHttpRequestHandler handler) {
        requestHandlers.put(normalizeRootDir(uriRoot), handler);
    }

    private String normalizeRootDir(String root) {
        Iterable<String> iter = Splitter.on('/').trimResults().omitEmptyStrings().split(root);

        StringBuilder builder = new StringBuilder();
        return builder.append('/').append(Joiner.on('/').skipNulls().join(iter)).append('/').toString();
    }

    private String match(String uri) {
        for (String root : requestHandlers.keySet()) {
            if (uri.startsWith(root)) {
                return root;
            }
        }

        return null;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (!request.decoderResult().isSuccess()) {
            LOG.error("Exception context[{}] http request decode error", ctx.toString());
            //请求解析失败
            ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.reasonPhrase());
            return;
        }

        String matchedUri = match(request.uri());
        if (matchedUri == null) {
            LOG.error("Exception context[{}] invalid uri[{}]", ctx.toString(), request.uri());
            ResponseSender.sendError(ctx, HttpResponseStatus.NOT_ACCEPTABLE, HttpResponseStatus.NOT_ACCEPTABLE.reasonPhrase());
            return;
        }

        NettyHttpRequestHandler requestHandler = requestHandlers.get(matchedUri);

        //删除context path，后续的handler可以更便捷的处理uri
        request.setUri(request.uri().substring(matchedUri.length() - 1));
        requestHandler.requestReceived(ctx, request);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        LOG.error("context[{}]", ctx.toString(), cause);
        if (ctx.channel().isActive()) {
            ResponseSender.sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.toString());
        }
    }
}
