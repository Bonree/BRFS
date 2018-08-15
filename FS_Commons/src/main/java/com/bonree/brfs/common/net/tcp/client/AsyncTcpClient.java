package com.bonree.brfs.common.net.tcp.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.google.common.base.Preconditions;

public class AsyncTcpClient implements TcpClient<BaseMessage, BaseResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncTcpClient.class);
	
	private Channel channel;
	
	private Executor executor;
	private AtomicInteger tokenMaker = new AtomicInteger(0);
	private ConcurrentHashMap<Integer, ResponseHandler<BaseResponse>> handlers;
	
	private TcpClientCloseListener listener;
	
	AsyncTcpClient(Executor executor) {
		this.executor = executor;
		this.handlers = new ConcurrentHashMap<>();
	}
	
	void attach(Channel channel) {
		Preconditions.checkNotNull(channel);
		this.channel = channel;
		this.channel.closeFuture().addListener(new ChannelCloseListener());
	}
	
	private int token() {
		return tokenMaker.getAndIncrement() & Integer.MAX_VALUE;
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
	public void sendMessage(BaseMessage msg, ResponseHandler<BaseResponse> handler) throws Exception {
		Preconditions.checkNotNull(msg);
		Preconditions.checkNotNull(handler);
		
		msg.setToken(token());
		handlers.put(msg.getToken(), handler);
		channel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(!future.isSuccess()) {
					handlers.remove(msg.getToken());
					executor.execute(new Runnable() {
						
						@Override
						public void run() {
							handler.error(new Exception("send message of type[" + msg.getType() + "] error"));
						}
					});
					
					channel.close();
				} else {
					LOG.info("send base message[{}, {}]", msg.getToken(), msg.getType());
				}
			}
		});
	}
	
	void handleResponse(BaseResponse response) {
		ResponseHandler<BaseResponse> handler = handlers.get(response.getToken());
		if(handler == null) {
			LOG.error("no handler is found for response[{}]", response.getToken());
			return;
		}
		
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				handler.handle(response);
			}
		});
	}

	@Override
	public void close() throws IOException {
		this.channel.close();
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
			
			for(ResponseHandler<BaseResponse> handler : handlers.values()) {
				executor.execute(new Runnable() {
					
					@Override
					public void run() {
						handler.error(new Exception("channel is closed!"));
					}
				});
			}
		}
		
	}
}
