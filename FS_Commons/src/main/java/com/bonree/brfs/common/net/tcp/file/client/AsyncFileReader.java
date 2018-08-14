package com.bonree.brfs.common.net.tcp.file.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.client.ResponseHandler;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientCloseListener;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.google.common.base.Preconditions;

public class AsyncFileReader implements TcpClient<ReadObject, FileContentPart> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncFileReader.class);
	
	private Channel channel;
	private Executor executor;
	
	private BlockingQueue<ResponseHandler<FileContentPart>> processorQueue = new LinkedBlockingQueue<ResponseHandler<FileContentPart>>();
	private ResponseHandler<FileContentPart> currentHandler;
	
	private TcpClientCloseListener listener;
	
	AsyncFileReader(Executor executor) {
		this.executor = executor;
	}
	
	void attach(Channel channel) {
		Preconditions.checkNotNull(channel);
		this.channel = channel;
		this.channel.closeFuture().addListener(new ChannelCloseListener());
	}
	
	@Override
	public String remoteHost() {
		InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
		return address.getHostString();
	}

	@Override
	public int remotePort() {
		InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
		return address.getPort();
	}

	@Override
	public void sendMessage(ReadObject object, ResponseHandler<FileContentPart> processor) throws InterruptedException {
		Preconditions.checkNotNull(object);
		Preconditions.checkNotNull(processor);
		
		processorQueue.put(processor);
		channel.writeAndFlush(object).addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(!future.isSuccess()) {
					processorQueue.remove(processor);
					executor.execute(new Runnable() {
						
						@Override
						public void run() {
							processor.error();
						}
					});
					
					close();
					return;
				}
			}
		});
	}
	
	void handleResponse(FileContentPart content) {
		if(currentHandler == null) {
			currentHandler = processorQueue.poll();
		}
		
		if(currentHandler == null) {
			throw new IllegalStateException("no handler to handle file content!");
		}
		
		final ResponseHandler<FileContentPart> handler = currentHandler;
		
		if(content.endOfContent()) {
			currentHandler = null;
		}
		
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				if(content.content() == null) {
					handler.error();
					return;
				}
				
				handler.handle(content);
			}
		});
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}
	
	@Override
	public void setClientCloseListener(TcpClientCloseListener listener) {
		this.listener = listener;
	}
	
	private class ChannelCloseListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			LOG.warn("channel closed!");
			if(listener != null) {
				executor.execute(new Runnable() {
					
					@Override
					public void run() {
						try {
							listener.clientClosed();
						} catch (Exception e) {
							LOG.error("call tcp client close listener error", e);
						}
					}
				});
			}
			
			ResponseHandler<FileContentPart> handler = null;
			while((handler = processorQueue.poll()) != null) {
				final ResponseHandler<FileContentPart> h = handler;
				executor.execute(new Runnable() {
					
					@Override
					public void run() {
						h.error();
					}
				});
			}
		}
		
	}
}
