package com.bonree.brfs.common.net.tcp.client;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.TokenMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMessageEncoder extends MessageToByteEncoder<TokenMessage<BaseMessage>> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, TokenMessage<BaseMessage> msg, ByteBuf out)
        throws Exception {
        out.writeByte((byte) 0xBF);
        out.writeInt(msg.messageToken());
        LOG.debug("encoding message with token [{}]", msg.messageToken());
        BaseMessage baseMessage = msg.message();
        out.writeByte(baseMessage.getType());

        int length = baseMessage.getBody() == null ? 0 : baseMessage.getBody().length;
        out.writeInt(length);

        if (length > 0) {
            out.writeBytes(baseMessage.getBody());
        }
    }

}
