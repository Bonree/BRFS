package com.bonree.brfs.common.net.tcp.file.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

import com.bonree.brfs.common.net.tcp.client.TcpResponseHandler;

public class FileContentHandler extends ByteToMessageDecoder {
	private int readingLength = 0;
	private BlockingQueue<TcpResponseHandler> processorQueue;
	private TcpResponseHandler currentProcessor;
	private Executor executor;
	
	public FileContentHandler(BlockingQueue<TcpResponseHandler> processorQueue, Executor executor) {
		this.processorQueue = processorQueue;
		this.executor = executor;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		if(readingLength == 0) {
			if(in.readableBytes() < Integer.BYTES) {
				return;
			}
			
			readingLength = in.readInt();
			currentProcessor = processorQueue.take();
			
			if(readingLength < 0) {
				executor.execute(new Runnable() {
					
					@Override
					public void run() {
						currentProcessor.fail();
					}
				});
				
				readingLength = 0;
				return;
			}
			
			if(readingLength == 0) {
				executor.execute(new Runnable() {
					
					@Override
					public void run() {
						currentProcessor.received(new byte[0], true);
					}
				});
				
				return;
			}
		}
		
		int readableLength = Math.min(readingLength, in.readableBytes());
		if(readableLength == 0) {
			return;
		}
		
		if(currentProcessor == null) {
			in.skipBytes(readableLength);
			readingLength -= readableLength;
			return;
		}
		
		byte[] bytes = new byte[readableLength];
		in.readBytes(bytes);
		readingLength -= readableLength;
		
		boolean endOfFile = readingLength == 0;
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				currentProcessor.received(bytes, endOfFile);
			}
		});
	}
}
