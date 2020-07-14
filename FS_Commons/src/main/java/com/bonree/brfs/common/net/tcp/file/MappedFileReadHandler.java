package com.bonree.brfs.common.net.tcp.file;

import com.bonree.brfs.common.net.Deliver;
import com.bonree.brfs.common.net.tcp.file.client.TimePair;
import com.bonree.brfs.common.statistic.ReadStatCollector;
import com.bonree.brfs.common.supervisor.ReadMetric;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.BufferUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MappedFileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
    private static final Logger log = LoggerFactory.getLogger(MappedFileReadHandler.class);
    private final ReadStatCollector readCountCollector;
    private final Deliver deliver;

    private final ReadObjectTranslator translator;

    private final LoadingCache<String, BufferHolder> bufferCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(3, TimeUnit.MINUTES)
            .refreshAfterWrite(5, TimeUnit.SECONDS)
            .removalListener((RemovalListener<String, BufferHolder>) notification -> {
                log.info("remove file mapping of [{}] from cache", notification.getKey());
                BufferHolder holder = notification.getValue();
                if (holder.isReloaded()) {
                    return;
                }

                holder.close();
            })
            .build(new CacheLoader<String, BufferHolder>() {

                @Override
                public BufferHolder load(String filePath) throws Exception {
                    log.info("loading file[{}] to memory...", filePath);
                    return new BufferHolder(Files.map(new File(filePath), MapMode.READ_ONLY));
                }

                @Override
                public ListenableFuture<BufferHolder> reload(String filePath, BufferHolder oldValue) throws Exception {
                    File file = new File(filePath);
                    if (oldValue.buffer().capacity() == file.length()) {
                        oldValue.reload();
                        return Futures.immediateFuture(oldValue);
                    }

                    log.info("reloading file[{}], old length[{}], new length[{}]",
                             filePath,
                             oldValue.buffer().capacity(),
                             file.length());

                    return Futures.immediateFuture(new BufferHolder(Files.map(file, MapMode.READ_ONLY)));
                }
            });

    private final LoadingCache<TimePair, String> timeCache;

    public MappedFileReadHandler(ReadObjectTranslator translator, Deliver deliver, LoadingCache<TimePair, String> timeCache,
                                 ReadStatCollector readCountCollector) {
        this.deliver = deliver;
        this.translator = translator;
        this.timeCache = timeCache;
        this.readCountCollector = readCountCollector;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ReadObject readObject) throws Exception {
        if (readObject.getFilePath().equals("-")) {
            readObject.setFilePath(TimeUtils.buildPath(readObject, timeCache));
        }

        ReadMetric readMetric = new ReadMetric();
        readMetric.setMonitorTime(System.currentTimeMillis());
        readMetric.setStorageName(readObject.getSn());
        readMetric.setDataNodeId(readObject.getFileName().split("_")[readObject.getIndex()]);
        readMetric.setDataCount(1);
        TimeWatcher timeWatcher = new TimeWatcher();
        String srName = getStorageName(readObject.getFilePath());

        String filePath = (readObject.getRaw() & ReadObject.RAW_PATH) == 0
            ? translator.filePath(readObject.getFilePath()) : readObject.getFilePath();

        try {
            BufferHolder holder = bufferCache.get(filePath);
            holder.increment();
            MappedByteBuffer fileBuffer = holder.buffer();

            long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) :
                readObject.getOffset();
            int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) :
                readObject.getLength();
            long fileLength = fileBuffer.capacity();
            if (readOffset < 0 || readOffset > fileLength) {
                log.error("unexcepted file[{}] offset : {}, file length : {}", filePath, readOffset, fileLength);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                    .addListener(ChannelFutureListener.CLOSE);
                return;
            }

            int readableLength = (int) Math.min(readLength, fileLength - readOffset);

            readMetric.setDataSize(readableLength);
            readCountCollector.submit(srName);

            ByteBuffer contentBuffer = fileBuffer.slice();
            contentBuffer.position((int) readOffset);
            contentBuffer.limit((int) (readOffset + readableLength));

            ByteBuf result = Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken())),
                                                    Unpooled.wrappedBuffer(Ints.toByteArray(readableLength)),
                                                    Unpooled.wrappedBuffer(contentBuffer.slice()));

            ctx.writeAndFlush(result)
                .addListener((ChannelFutureListener) future -> {
                    readMetric.setElapsedTime(timeWatcher.getElapsedTime());
                    deliver.sendReaderMetric(readMetric.toMap());

                    holder.decrement();
                })
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        } catch (ExecutionException e) {
            log.error("can not open file channel for {}", filePath, e);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                .addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e) {
            log.error("read file error", e);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private String getStorageName(String filePath) {
        String[] split = filePath.split("/");
        if (split.length < 2) {
            return "";
        }
        return split[1];
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("file read error", cause);
        ctx.close();
    }

    private static class BufferHolder implements Closeable {
        private final MappedByteBuffer buffer;
        private AtomicInteger refCount = new AtomicInteger();
        private volatile boolean close;
        private AtomicBoolean reloaded = new AtomicBoolean(false);

        public BufferHolder(MappedByteBuffer buffer) {
            this.buffer = buffer;
        }

        public void reload() {
            reloaded.set(true);
        }

        public boolean isReloaded() {
            return reloaded.compareAndSet(true, false);
        }

        public MappedByteBuffer buffer() {
            return buffer;
        }

        public void increment() {
            this.refCount.incrementAndGet();
        }

        public void decrement() {
            if (refCount.decrementAndGet() <= 0 && close) {
                BufferUtils.release(buffer);
            }
        }

        @Override
        public void close() {
            close = true;
            if (refCount.compareAndSet(0, -1)) {
                BufferUtils.release(buffer);
            }
        }
    }
}