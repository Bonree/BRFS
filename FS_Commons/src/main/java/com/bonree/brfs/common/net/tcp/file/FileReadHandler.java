package com.bonree.brfs.common.net.tcp.file;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;

import com.google.common.primitives.Ints;

@Sharable
public class FileReadHandler extends SimpleChannelInboundHandler<ReadObject> {
	private ReadObjectTranslator translator;
	
	public FileReadHandler(ReadObjectTranslator translator) {
		this.translator = translator;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ReadObject readObject)throws Exception {
		File file = new File((readObject.getRaw() & ReadObject.RAW_PATH) == 0 ? translator.filePath(readObject.getFilePath()) : readObject.getFilePath());
		if(!file.exists() || !file.isFile()) {
			throw new IllegalArgumentException("unexcepted file path : " + file.getAbsolutePath());
		}
		
		long readOffset = (readObject.getRaw() & ReadObject.RAW_OFFSET) == 0 ? translator.offset(readObject.getOffset()) : readObject.getOffset();
		int readLength = (readObject.getRaw() & ReadObject.RAW_LENGTH) == 0 ? translator.length(readObject.getLength()) : readObject.getLength();
		long fileLength = file.length();
		if(readOffset < 0 || readOffset > fileLength) {
			throw new IllegalArgumentException("unexcepted file offset : " + readOffset);
		}
		
		int readableLength = (int) Math.min(readLength, fileLength - readOffset);
		ctx.write(Unpooled.wrappedBuffer(Ints.toByteArray(readableLength)));
		
//		ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[readableLength])).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
		
		//zero-copy read
        ctx.writeAndFlush(new DefaultFileRegion(file, readOffset, readableLength)).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        
        //normal read
//        ctx.writeAndFlush(Files.asByteSource(file).slice(readObject.getOffset(), readableLength).read()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.writeAndFlush(Unpooled.wrappedBuffer(Ints.toByteArray(-1)));
		return;
	}

}
