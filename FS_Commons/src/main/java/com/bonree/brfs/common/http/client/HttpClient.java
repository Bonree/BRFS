package com.bonree.brfs.common.http.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.client.util.HttpAsyncClientUtils;
import org.apache.http.protocol.HttpContext;

public class HttpClient implements Closeable {
	
	private CloseableHttpAsyncClient client;
	private ClientConfig clientConfig;
	
	public HttpClient() {
		this(ClientConfig.DEFAULT);
	}
	
	public HttpClient(ClientConfig clientConfig) {
		this.clientConfig = clientConfig;
		
		ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setBufferSize(clientConfig.getBufferSize())
                .setCharset(Consts.UTF_8)
                .build();
		
		IOReactorConfig ioConfig = IOReactorConfig.custom()
				.setSoKeepAlive(true)
				.setConnectTimeout(clientConfig.getConnectTimeout())
				.setSndBufSize(clientConfig.getSocketSendBufferSize())
				.setRcvBufSize(clientConfig.getSocketRecvBufferSize())
				.setIoThreadCount(clientConfig.getIOThreadNum())
				.setTcpNoDelay(false)
				.build();
		
		List<Header> defaultHeaders = new ArrayList<Header>();
		defaultHeaders.add(new BasicHeader("Connection", "keep-alive"));
		
		client = HttpAsyncClientBuilder.create()
		           .setMaxConnPerRoute(clientConfig.getMaxConnectionPerRoute())
		           .setMaxConnTotal(clientConfig.getMaxConnection())
		           .setDefaultConnectionConfig(connectionConfig)
		           .setDefaultIOReactorConfig(ioConfig)
		           .setConnectionReuseStrategy(new ConnectionReuseStrategy() {

					@Override
					public boolean keepAlive(org.apache.http.HttpResponse response, HttpContext context) {
						return true;
					}
					
				})
				.setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
					
					@Override
					public long getKeepAliveDuration(org.apache.http.HttpResponse response, HttpContext context) {
						return clientConfig.getIdleTimeout();
					}
					
				})
				.setDefaultHeaders(defaultHeaders)
				.build();
		
		client.start();
	}
	
	public HttpResponse executeGet(URI uri) throws Exception {
		return executeInner(new HttpGet(uri));
	}
	
	public HttpResponse executeGet(URI uri, Map<String, String> headers) throws Exception {
		HttpGet httpGet = new HttpGet(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			httpGet.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(httpGet);
	}
	
	public void executeGet(URI uri, ResponseHandler handler) {
		executeInner(new HttpGet(uri), handler);
	}
	
	public void executeGet(URI uri, Map<String, String> headers, ResponseHandler handler) {
		HttpGet httpGet = new HttpGet(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			httpGet.setHeader(entry.getKey(), entry.getValue());
		}
		
		executeInner(httpGet, handler);
	}
	
	public HttpResponse executePut(URI uri) throws Exception {
		return executeInner(new HttpPut(uri));
	}
	
	public HttpResponse executePut(URI uri, Map<String, String> headers) throws Exception {
		HttpPut httpPut = new HttpPut(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			httpPut.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(httpPut);
	}
	
	public void executePut(URI uri, ResponseHandler handler) {
		executeInner(new HttpPut(uri), handler);
	}
	
	public void executePut(URI uri, Map<String, String> headers, ResponseHandler handler) {
		HttpPut httpPut = new HttpPut(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			httpPut.setHeader(entry.getKey(), entry.getValue());
		}
		
		executeInner(httpPut, handler);
	}
	
	public HttpResponse executePut(URI uri, byte[] bytes) throws Exception {
		HttpPut put = new HttpPut(uri);
		put.setEntity(new ByteArrayEntity(bytes));
		
		return executeInner(put);
	}
	
	public HttpResponse executePut(URI uri, Map<String, String> headers, byte[] bytes) throws Exception {
		HttpPut put = new HttpPut(uri);
		put.setEntity(new ByteArrayEntity(bytes));
		
		for(Entry<String, String> entry : headers.entrySet()) {
			put.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(put);
	}
	
	public void executePut(URI uri, byte[] bytes, ResponseHandler handler) {
		HttpPut put = new HttpPut(uri);
		put.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(put, handler);
	}
	
	public void executePut(URI uri, Map<String, String> headers, byte[] bytes, ResponseHandler handler) {
		HttpPut put = new HttpPut(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			put.setHeader(entry.getKey(), entry.getValue());
		}
		
		put.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(put, handler);
	}
	
	public HttpResponse executePost(URI uri) throws Exception {
		return executeInner(new HttpPost(uri));
	}
	
	public HttpResponse executePost(URI uri, Map<String, String> headers) throws Exception {
		HttpPost post = new HttpPost(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			post.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(post);
	}
	
	public void executePost(URI uri, ResponseHandler handler) {
		executeInner(new HttpPost(uri), handler);
	}
	
	public void executePost(URI uri, Map<String, String> headers, ResponseHandler handler) {
		HttpPost post = new HttpPost(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			post.setHeader(entry.getKey(), entry.getValue());
		}
		
		executeInner(post, handler);
	}
	
	public HttpResponse executePost(URI uri, byte[] bytes) throws Exception {
		HttpPost post = new HttpPost(uri);
		post.setEntity(new ByteArrayEntity(bytes));
		
		return executeInner(post);
	}
	
	public HttpResponse executePost(URI uri, Map<String, String> headers, byte[] bytes) throws Exception {
		HttpPost post = new HttpPost(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			post.setHeader(entry.getKey(), entry.getValue());
		}
		
		post.setEntity(new ByteArrayEntity(bytes));
		
		return executeInner(post);
	}
	
	public void executePost(URI uri, byte[] bytes, ResponseHandler handler) {
		HttpPost post = new HttpPost(uri);
		post.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(post, handler);
	}
	
	public void executePost(URI uri, Map<String, String> headers, byte[] bytes, ResponseHandler handler) {
		HttpPost post = new HttpPost(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			post.setHeader(entry.getKey(), entry.getValue());
		}
		
		post.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(post, handler);
	}
	
	public HttpResponse executeClose(URI uri) throws Exception {
		return executeInner(new HttpClose(uri));
	}
	
	public HttpResponse executeClose(URI uri, Map<String, String> headers) throws Exception {
		HttpClose close = new HttpClose(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			close.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(close);
	}
	
	public void executeClose(URI uri, ResponseHandler handler) {
		executeInner(new HttpClose(uri), handler);
	}
	
	public void executeClose(URI uri, Map<String, String> headers, ResponseHandler handler) {
		HttpClose close = new HttpClose(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			close.setHeader(entry.getKey(), entry.getValue());
		}
		
		executeInner(close, handler);
	}
	
	public HttpResponse executeClose(URI uri, byte[] bytes) throws Exception {
		HttpClose close = new HttpClose(uri);
		close.setEntity(new ByteArrayEntity(bytes));
		
		return executeInner(close);
	}
	
	public HttpResponse executeClose(URI uri, Map<String, String> headers, byte[] bytes) throws Exception {
		HttpClose close = new HttpClose(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			close.setHeader(entry.getKey(), entry.getValue());
		}
		close.setEntity(new ByteArrayEntity(bytes));
		
		return executeInner(close);
	}
	
	public void executeClose(URI uri, byte[] bytes, ResponseHandler handler) {
		HttpClose close = new HttpClose(uri);
		close.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(close, handler);
	}
	
	public void executeClose(URI uri, Map<String, String> headers, byte[] bytes, ResponseHandler handler) {
		HttpClose close = new HttpClose(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			close.setHeader(entry.getKey(), entry.getValue());
		}
		
		close.setEntity(new ByteArrayEntity(bytes));
		
		executeInner(close, handler);
	}
	
	public HttpResponse executeDelete(URI uri) throws Exception {
		return executeInner(new HttpDelete(uri));
	}
	
	public HttpResponse executeDelete(URI uri, Map<String, String> headers) throws Exception {
		HttpDelete delete = new HttpDelete(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			delete.setHeader(entry.getKey(), entry.getValue());
		}
		
		return executeInner(delete);
	}
	
	public void executeDelete(URI uri, ResponseHandler handler) {
		executeInner(new HttpDelete(uri), handler);
	}
	
	public void executeDelete(URI uri, Map<String, String> headers, ResponseHandler handler) {
		HttpDelete delete = new HttpDelete(uri);
		for(Entry<String, String> entry : headers.entrySet()) {
			delete.setHeader(entry.getKey(), entry.getValue());
		}
		
		executeInner(delete, handler);
	}
	
	private HttpResponse executeInner(HttpUriRequest request) throws Exception {
		Future<org.apache.http.HttpResponse> future = client.execute(request, new FutureCallback<org.apache.http.HttpResponse>() {

			@Override
			public void completed(org.apache.http.HttpResponse result) {
			}

			@Override
			public void failed(Exception ex) {
			}

			@Override
			public void cancelled() {
			}
		});
		
		return new HttpResponseProxy(future.get(clientConfig.getResponseTimeout(), TimeUnit.MILLISECONDS));
	}
	
	private void executeInner(HttpUriRequest request, ResponseHandler handler) {
		client.execute(request, new HttpResponseReceiver(handler));
	}
	
	private class HttpResponseReceiver implements FutureCallback<org.apache.http.HttpResponse> {
		private ResponseHandler responseHandler;
		
		public HttpResponseReceiver(ResponseHandler responseHandler) {
			this.responseHandler = responseHandler;
		}

		@Override
		public void completed(org.apache.http.HttpResponse response) {
			responseHandler.onCompleted(new HttpResponseProxy(response));
		}

		@Override
		public void failed(Exception ex) {
			responseHandler.onThrowable(ex);
		}

		@Override
		public void cancelled() {
			responseHandler.onThrowable(new CancellationException());
		}
		
	}

	@Override
	public void close() throws IOException {
		HttpAsyncClientUtils.closeQuietly(client);
	}
}
