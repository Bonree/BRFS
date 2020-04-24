package com.bonree.brfs.common.net.tcp.file.client;

import com.bonree.brfs.common.net.tcp.TokenMessage;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ReadObjectEncoder extends MessageToByteEncoder<TokenMessage<ReadObject>> {

    @Override
    protected void encode(ChannelHandlerContext ctx, TokenMessage<ReadObject> object, ByteBuf out)
        throws Exception {
        ReadObject readObject = object.message();

        readObject.setToken(object.messageToken());

        out.writeBytes(Joiner.on(';').useForNull("-")
                           .join(readObject.getSn(),
                                 readObject.getIndex(),
                                 readObject.getTime(),
                                 readObject.getDuration(),
                                 readObject.getFileName(),
                                 readObject.getFilePath(),
                                 readObject.getOffset(),
                                 readObject.getLength(),
                                 readObject.getRaw(),
                                 readObject.getToken(),
                                 "\n").getBytes(Charsets.UTF_8));
    }

}
