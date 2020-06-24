package com.bonree.brfs.common.net.tcp.client;

import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.TokenMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseResponseDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(BaseResponseDecoder.class);
    private TokenMessage<BaseResponse> response;


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (response == null) {
            if (in.readableBytes() < Integer.BYTES * 3) {
                return;
            }

            final int token = in.readInt();
            LOG.info("decoding message with token [{}]", token);
            final int code = in.readInt();
            final int length = in.readInt();

            response = new TokenMessage<BaseResponse>(token, new BaseResponse(code));

            if (length == 0) {
                out.add(response);
                response = null;
                return;
            }

            response.message().setBody(new byte[length]);
        }

        if (in.readableBytes() < response.message().getBody().length) {
            return;
        }

        in.readBytes(response.message().getBody());
        out.add(response);
        response = null;
    }

}
