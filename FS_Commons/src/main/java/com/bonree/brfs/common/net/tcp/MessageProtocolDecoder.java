package com.bonree.brfs.common.net.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProtocolDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(MessageProtocolDecoder.class);

    private static final byte HEADER_BYTE = (byte) 0xBF;
    private static final int HEADER_LENGTH = 10;

    private TokenMessage<BaseMessage> decodingMessage;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (decodingMessage == null) {
            if (in.readableBytes() < HEADER_LENGTH) {
                return;
            }

            if (in.readByte() != HEADER_BYTE) {
                throw new IllegalArgumentException("Header byte of mesage is illegal.");
            }

            final int token = in.readInt();
            final int type = in.readByte();
            final int length = in.readInt();
            LOG.debug("decoding message with token [{}], type[{}], length[{}]", token, type, length);
            if (type == -1) {
                //心跳消息
                return;
            }
            decodingMessage = new TokenMessage<BaseMessage>(token, new BaseMessage(type));

            if (length == 0) {
                out.add(decodingMessage);
                decodingMessage = null;
                return;
            }

            decodingMessage.message().setBody(new byte[length]);
        }

        byte[] body = decodingMessage.message().getBody();
        if (body == null) {
            LOG.error("NONONO, body is null!!!");
        }

        if (in.readableBytes() < body.length) {
            return;
        }

        in.readBytes(body);
        out.add(decodingMessage);
        decodingMessage = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("message protocol decoder failed!", cause);
        ctx.channel().close().sync();
    }
}
