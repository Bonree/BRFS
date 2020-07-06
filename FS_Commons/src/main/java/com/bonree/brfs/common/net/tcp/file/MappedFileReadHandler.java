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
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class MappedFileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
    private static final Logger LOG = LoggerFactory.getLogger(MappedFileReadHandler.class);
    private ReadStatCollector readCountCollector;
    private final Deliver deliver;

    private ReadObjectTranslator translator;
    private ExecutorService releaseRunner = Executors.newSingleThreadExecutor(new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "map_release_t");
            thread.setDaemon(true);

            return thread;
        }
    });

    private LinkedList<BufferRef> releaseList = new LinkedList<>();
    private LoadingCache<String, BufferRef> bufferCache =
        CacheBuilder.newBuilder()
            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
            .maximumSize(64)
            .initialCapacity(32)
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, BufferRef>() {

                @Override
                public void onRemoval(RemovalNotification<String, BufferRef> notification) {
                    LOG.info("remove file mapping of [{}] from cache", notification.getKey());
                    synchronized (releaseList) {
                        releaseList.addLast(notification.getValue());
                    }
                }
            })
            .build(new CacheLoader<String, BufferRef>() {

                @Override
                public BufferRef load(String filePath) throws Exception {
                    LOG.info("loading file[{}] to memory...", filePath);
                    return new BufferRef(Files.map(new File(filePath), MapMode.READ_ONLY));
                }

            });

    private LoadingCache<TimePair, String> timeCache;

    public MappedFileReadHandler(ReadObjectTranslator translator, Deliver deliver, LoadingCache<TimePair, String> timeCache,
                                 ReadStatCollector readCountCollector) {
        this.deliver = deliver;
        this.translator = translator;
        this.timeCache = timeCache;
        this.readCountCollector = readCountCollector;
        this.releaseRunner.execute(() -> {
            while (true) {
                Iterator<BufferRef> iter = releaseList.iterator();
                while (iter.hasNext()) {
                    BufferRef bufferRef = iter.next();
                    if (bufferRef.refCount() == 0) {
                        BufferUtils.release(bufferRef.buffer());
                        iter.remove();
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
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

        MappedByteBuffer fileBuffer = null;
        try {
            BufferRef ref = bufferCache.get(filePath).retain();
            fileBuffer = ref.buffer();

            long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) :
                readObject.getOffset();
            int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) :
                readObject.getLength();
            long fileLength = fileBuffer.capacity();
            if (readOffset < 0 || readOffset > fileLength) {
                LOG.error("unexcepted file[{}] offset : {}, file length : {}", filePath, readOffset, fileLength);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
                    .addListener(ChannelFutureListener.CLOSE);
                return;
            }

            int readableLength = (int) Math.min(readLength, fileLength - readOffset);
            if (readableLength == 0) {
                bufferCache.invalidate(filePath);
                fileBuffer = ref.buffer();
            }

            readMetric.setDataSize(readableLength);
            readCountCollector.submit(srName);

            ByteBuffer contentBuffer = fileBuffer.slice();
            contentBuffer.position((int) readOffset);
            contentBuffer.limit((int) (readOffset + readableLength));

            ByteBuf result = Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken())),
                                                    Unpooled.wrappedBuffer(Ints.toByteArray(readableLength)),
                                                    Unpooled.wrappedBuffer(contentBuffer.slice()));

            ctx.writeAndFlush(result).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    readMetric.setElapsedTime(timeWatcher.getElapsedTime());
                    ref.release();

                    deliver.sendReaderMetric(readMetric.toMap());
                }
            }).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
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
            bufferCache.cleanUp();
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
        LOG.error("file read error", cause);
        ctx.close();
    }

    private class BufferRef {
        private AtomicInteger refCount;
        private final MappedByteBuffer buffer;

        public BufferRef(MappedByteBuffer buffer) {
            this.buffer = buffer;
            this.refCount = new AtomicInteger(0);
        }

        public MappedByteBuffer buffer() {
            return this.buffer;
        }

        public int refCount() {
            return this.refCount.get();
        }

        public BufferRef retain() {
            refCount.incrementAndGet();
            return this;
        }

        public boolean release() {
            return refCount.decrementAndGet() == 0;
        }
    }
}