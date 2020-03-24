package com.bonree.brfs.duplication.rocksdb.tmp;

import com.bonree.brfs.common.net.http.netty.ResponseSender;
import com.bonree.brfs.duplication.rocksdb.RocksDBManager;
import com.bonree.brfs.duplication.rocksdb.WriteStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MemoryAttribute;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ChannelHandler.Sharable
public class RocksDBHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBHandler.class);

    private RocksDBManager rocksDBManager;

    public RocksDBHandler(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {

        if (!request.decoderResult().isSuccess()) {
            LOG.error("Exception context[{}] http request decode error", ctx.toString());
            //请求解析失败
            ResponseSender.sendError(ctx, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.reasonPhrase());
            return;
        }

        String reqUri = request.uri();
        if (reqUri == null || reqUri.isEmpty()) {
            LOG.error("Exception context[{}] invalid uri[{}]", ctx.toString(), request.uri());
            ResponseSender.sendError(ctx, HttpResponseStatus.NOT_ACCEPTABLE, HttpResponseStatus.NOT_ACCEPTABLE.reasonPhrase());
            return;
        }

        Map<String, String> params = parseHttpReqParam(request);
        LOG.info("req params:{}", params);
        HttpResponseStatus status = null;
        String content = null;
        if (reqUri.startsWith("/write")) {
            WriteStatus writeStatus = this.rocksDBManager.write(params.get("cf"), params.get("key").getBytes(), params.get("value").getBytes());
            status = writeStatus == WriteStatus.SUCCESS ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
            content = writeStatus == WriteStatus.SUCCESS ? "write success" : "write failed";
        } else if (reqUri.startsWith("/read")) {
            byte[] result = this.rocksDBManager.read(params.get("cf"), params.get("key").getBytes());
            status = result != null ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
            content = result != null ? new String(result) : "null";
        } else if (reqUri.startsWith("/addcf")) {
            this.rocksDBManager.createColumnFamilyWithTtl(params.get("cf"), Integer.parseInt(params.get("ttl")));
            status = HttpResponseStatus.OK;
            content = "OK";
        } else if (reqUri.startsWith("/delcf")) {
            this.rocksDBManager.deleteColumnFamily(params.get("cf"));
            status = HttpResponseStatus.OK;
            content = "OK";
        } else {
            LOG.info("UnSupport request type:{}", reqUri);
            ResponseSender.sendError(ctx, HttpResponseStatus.NOT_ACCEPTABLE, HttpResponseStatus.NOT_ACCEPTABLE.reasonPhrase());
            return;
        }

        ByteBuf buf = Unpooled.copiedBuffer(content, CharsetUtil.UTF_8);

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        ResponseSender.sendResponse(ctx, response);

    }

    public static Map<String, String> parseHttpReqParam(FullHttpRequest req) {
        Map<String, String> params = new HashMap<>();

        // GET请求
        if (HttpMethod.GET.equals(req.method())) {
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(req.getUri());
            Map<String, List<String>> paramMap = queryStringDecoder.parameters();
            for (Map.Entry<String, List<String>> entry : paramMap.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    params.put(entry.getKey(), entry.getValue().get(0));
                }
            }
        }

        // POST请求
        if (HttpMethod.POST.equals(req.method())) {
            HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), req);
            List<InterfaceHttpData> postList = decoder.getBodyHttpDatas();
            for (InterfaceHttpData data : postList) {
                if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
                    MemoryAttribute attribute = (MemoryAttribute) data;
                    params.put(attribute.getName(), attribute.getValue());
                }
            }
        }
        return params;
    }
}
