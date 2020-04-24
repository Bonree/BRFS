package com.bonree.brfs.common.net.tcp.file;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class SimpleFileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileReadHandler.class);

    private ReadObjectTranslator translator;

    public SimpleFileReadHandler(ReadObjectTranslator translator) {
        this.translator = translator;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ReadObject readObject) throws Exception {
        String filePath = (readObject.getRaw() & ReadObject.RAW_PATH) == 0 ?
            translator.filePath(readObject.getFilePath()) : readObject.getFilePath();

        try {
            File file = new File(filePath);

            long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) :
                readObject.getOffset();
            int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) :
                readObject.getLength();
            long fileLength = file.length();
            if (readOffset < 0 || readOffset > fileLength) {
                LOG.error("unexcepted file offset : {}", readOffset);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                   .addListener(ChannelFutureListener.CLOSE);
                return;
            }

            int readableLength = (int) Math.min(readLength, fileLength - readOffset);

            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()),
                                                     Ints.toByteArray(readableLength),
                                                     Files.asByteSource(file).slice(readOffset, readableLength).read()))
               .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        } catch (Exception e) {
            LOG.error("read file error", e);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
               .addListener(ChannelFutureListener.CLOSE);
            return;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("file read error", cause);
        ctx.close();
    }

}