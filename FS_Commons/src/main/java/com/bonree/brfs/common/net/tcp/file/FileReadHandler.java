package com.bonree.brfs.common.net.tcp.file;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		File file = new File((readObject.getRaw() & ReadObject.RAW_PATH) == 0 ? translator.filePath(readObject.getFilePath()) : readObject.getFilePath());
		if(!file.exists() || !file.isFile()) {
			LOG.error("unexcepted file path : {}", file.getAbsolutePath());
			ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
			.addListener(ChannelFutureListener.CLOSE);
			return;
		}
		
		long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) : readObject.getOffset();
		int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) : readObject.getLength();
		long fileLength = file.length();
		if(readOffset < 0 || readOffset > fileLength) {
			LOG.error("unexcepted file offset : {}", readOffset);
			ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(-1)))
			.addListener(ChannelFutureListener.CLOSE);
			return;
		}
		
		int readableLength = (int) Math.min(readLength, fileLength - readOffset);
		ctx.write(Unpooled.wrappedBuffer(Bytes.concat(Ints.toByteArray(readObject.getToken()), Ints.toByteArray(readableLength))));
		
		//zero-copy read
        ctx.writeAndFlush(new DefaultFileRegion(file, readOffset, readableLength)).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        
        //normal read
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(Files.asByteSource(file).slice(readOffset, readableLength).read())).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.close();
	}

}
