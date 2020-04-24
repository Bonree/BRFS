package com.bonree.brfs.common.net.tcp.file;

import com.bonree.brfs.common.net.tcp.file.client.TimePair;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.primitives.Ints;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class ZeroCopyFileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
    private static final Logger LOG = LoggerFactory.getLogger(ZeroCopyFileReadHandler.class);

    private ReadObjectTranslator translator;
    private LoadingCache<TimePair, String> timeCache;
    private LoadingCache<String, FileChannel> channelCache = (LoadingCache<String, FileChannel>) CacheBuilder.newBuilder()
                                                                                                             .concurrencyLevel(
                                                                                                                 Runtime
                                                                                                                     .getRuntime()
                                                                                                                     .availableProcessors())
                                                                                                             .maximumSize(200)
                                                                                                             .initialCapacity(50)
                                                                                                             .expireAfterAccess(
                                                                                                                 30,
                                                                                                                 TimeUnit.SECONDS)
                                                                                                             .removalListener(
                                                                                                                 new RemovalListener<String, FileChannel>() {

                                                                                                                     @Override
                                                                                                                     public void onRemoval(
                                                                                                                         RemovalNotification<String, FileChannel> notification) {
                                                                                                                         LOG.debug(
                                                                                                                             "close file channel {}",
                                                                                                                             notification
                                                                                                                                 .getValue());
                                                                                                                         CloseUtils
                                                                                                                             .closeQuietly(
                                                                                                                                 notification
                                                                                                                                     .getValue());
                                                                                                                     }
                                                                                                                 })
                                                                                                             .build(
                                                                                                                 new CacheLoader<String, FileChannel>() {

                                                                                                                     @SuppressWarnings("resource")
                                                                                                                     @Override
                                                                                                                     public FileChannel load(
                                                                                                                         String filePath)
                                                                                                                         throws
                                                                                                                         Exception {
                                                                                                                         FileChannel
                                                                                                                             channel =
                                                                                                                             new RandomAccessFile(
                                                                                                                                 filePath,
                                                                                                                                 "r")
                                                                                                                                 .getChannel();
                                                                                                                         LOG.debug(
                                                                                                                             "open file channel {} for {}",
                                                                                                                             channel,
                                                                                                                             filePath);
                                                                                                                         return channel;
                                                                                                                     }

                                                                                                                 });

    public ZeroCopyFileReadHandler(ReadObjectTranslator translator, LoadingCache<TimePair, String> timeCache) {
        this.translator = translator;
        this.timeCache = timeCache;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ReadObject readObject) throws Exception {
        if (readObject.getFilePath().equals("-")) {
            readObject.setFilePath(TimeUtils.buildPath(readObject, timeCache));
        }
        String filePath = (readObject.getRaw() & ReadObject.RAW_PATH) == 0 ?
            translator.filePath(readObject.getFilePath()) : readObject.getFilePath();

        FileChannel fileChannel = null;
        try {
            LOG.info("filepath = [{}]", filePath);
            fileChannel = channelCache.get(filePath);

            long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) :
                readObject.getOffset();
            int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) :
                readObject.getLength();
            long fileLength = fileChannel.size();
            if (readOffset < 0 || readOffset > fileLength) {
                LOG.error("unexcepted file offset : {}", readOffset);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                   .addListener(ChannelFutureListener.CLOSE);
                return;
            }

            int readableLength = (int) Math.min(readLength, fileLength - readOffset);

            ctx.write(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(readableLength)));

            //zero-copy read
            ctx.writeAndFlush(new WrapperedFileRegion(fileChannel, readOffset, readableLength))
               .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        } catch (ExecutionException e) {
            LOG.error("can not open file channel for {}", filePath, e);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
               .addListener(ChannelFutureListener.CLOSE);
            return;
        } catch (Exception e) {
            LOG.error("read file error", e);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
               .addListener(ChannelFutureListener.CLOSE);
            return;
        } finally {
            channelCache.cleanUp();
        }

        //		ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()),
        //				Ints.toByteArray(readObject.getLength()), new byte[readObject.getLength()]));

        //normal read
        //		FileContent content = FileDecoder.contents(Files.asByteSource(file).slice(readOffset, readableLength).read());
        //		byte[] bytes = content.getData().toByteArray();
        //		ctx.write(Unpooled.wrappedBuffer(Bytes.concat(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(bytes.length))));
        //        ctx.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("file read error", cause);
        ctx.close();
    }
}
