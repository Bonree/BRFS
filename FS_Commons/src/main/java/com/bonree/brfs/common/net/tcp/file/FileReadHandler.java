package com.bonree.brfs.common.net.tcp.file;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.CloseUtils;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

@Sharable
public class FileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
	private static final Logger LOG = LoggerFactory.getLogger(FileReadHandler.class);
	
	private ReadObjectTranslator translator;
	
	public FileReadHandler(ReadObjectTranslator translator) {
		this.translator = translator;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ReadObject readObject)throws Exception {
		String filePath = (readObject.getRaw() & ReadObject.RAW_PATH) == 0 ?
				translator.filePath(readObject.getFilePath()) : readObject.getFilePath();
				
		FileChannel fileChannel = null;
		try {
			fileChannel = new RandomAccessFile(filePath, "r").getChannel();
			
			long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) : readObject.getOffset();
			int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) : readObject.getLength();
			long fileLength = fileChannel.size();
			if(readOffset < 0 || readOffset > fileLength) {
				LOG.error("unexcepted file offset : {}", readOffset);
				ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
				.addListener(ChannelFutureListener.CLOSE);
				return;
			}
			
			int readableLength = (int) Math.min(readLength, fileLength - readOffset);
		} catch (Exception e) {
			LOG.error("read file error", e);
			ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
			.addListener(ChannelFutureListener.CLOSE);
			return;
		} finally {
			CloseUtils.closeQuietly(fileChannel);
		}
		
		ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()),
				Ints.toByteArray(readObject.getLength()), new byte[readObject.getLength()]));
//		ctx.write(Unpooled.wrappedBuffer(Bytes.concat(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(readableLength))));
//		
//		//zero-copy read
//        ctx.writeAndFlush(new DefaultFileRegion(file, readOffset, readableLength)).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        
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
