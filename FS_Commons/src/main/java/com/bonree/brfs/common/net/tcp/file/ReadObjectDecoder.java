package com.bonree.brfs.common.net.tcp.file;

import com.bonree.brfs.common.utils.JsonUtils;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

public class ReadObjectDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ReadObject object = null;
        if (in.hasArray()) {
            object = JsonUtils.toObject(in.array(), ReadObject.class);
        } else {
            byte[] bs = new byte[in.readableBytes()];
            in.readBytes(bs);
            object = JsonUtils.toObject(bs, ReadObject.class);
        }

        if (object == null) {
            throw new IllegalArgumentException();
        }

        out.add(object);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        cause.printStackTrace();
        ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(-1))).addListener(ChannelFutureListener.CLOSE);
    }

}
