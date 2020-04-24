package com.bonree.brfs.common.net.tcp.file;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class ReadObjectStringDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        List<String> parts = Splitter.on(';').splitToList(in.toString(Charsets.UTF_8));
        in.skipBytes(in.readableBytes());

        ReadObject object = new ReadObject();
        int index = 0;
        object.setSn(parts.get(index++));
        object.setIndex(Integer.parseInt(parts.get(index++)));
        object.setTime(Long.parseLong(parts.get(index++)));
        object.setDuration(Long.parseLong(parts.get(index++)));
        object.setFileName(parts.get(index++));
        object.setFilePath(parts.get(index++));
        object.setOffset(Long.parseLong(parts.get(index++)));
        object.setLength(Integer.parseInt(parts.get(index++)));
        object.setRaw(Integer.parseInt(parts.get(index++)));
        object.setToken(Integer.parseInt(parts.get(index++)));

        out.add(object);
    }

}
