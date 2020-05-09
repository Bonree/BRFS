package com.bonree.brfs.common.net.tcp.file;

import com.bonree.brfs.common.net.Deliver;
import com.bonree.brfs.common.net.tcp.file.client.TimePair;
import com.bonree.brfs.common.statistic.ReadStatCollector;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileChannelInitializer extends ChannelInitializer<SocketChannel> {
    private SimpleChannelInboundHandler fileReadHandler;
    private static boolean useZeroCopy = Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_READ_BY_ZEROCOPY);
    private LoadingCache<TimePair, String> timeCache = CacheBuilder.newBuilder()
        .maximumSize(1024).build(new CacheLoader<TimePair, String>() {

            @Override
            public String load(TimePair pair) throws Exception {
                return TimeUtils.timeInterval(pair.getTime(), pair.getDuration());
            }
        });

    public FileChannelInitializer(ReadObjectTranslator translator, Deliver deliver, ReadStatCollector readStatCollector) {
        if (useZeroCopy) {
            this.fileReadHandler = new ZeroCopyFileReadHandler(translator, timeCache, readStatCollector);
        } else {
            this.fileReadHandler = new MappedFileReadHandler(translator, deliver, timeCache, readStatCollector);
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new LineBasedFrameDecoder(1024 * 16));
        pipeline.addLast(new ReadObjectStringDecoder());
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(fileReadHandler);
    }

}
