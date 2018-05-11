package com.bonree.brfs.common.http.client;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import com.bonree.brfs.common.utils.CloseUtils;

public class HttpClient implements Closeable {
	
	private CloseableHttpClient client;
	
	public HttpClient() {
		this(ClientConfig.DEFAULT);
	}
	
	public HttpClient(ClientConfig clientConfig) {
		ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setBufferSize(clientConfig.getBufferSize())
                .setCharset(Consts.UTF_8)
                .build();
		
		SocketConfig socketConfig = SocketConfig.custom()
				.setRcvBufSize(clientConfig.getSocketRecvBufferSize())
				.setSndBufSize(clientConfig.getSocketSendBufferSize())
				.setSoKeepAlive(true)
				.setSoTimeout(clientConfig.getSocketTimeout())
				.setTcpNoDelay(true)
				.build();
				
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setDefaultSocketConfig(socketConfig);
		connectionManager.setDefaultConnectionConfig(connectionConfig);
		connectionManager.setDefaultMaxPerRoute(clientConfig.getMaxConnection());
		connectionManager.setMaxTotal(clientConfig.getMaxConnection());
		
		List<Header> defaultHeaders = new ArrayList<Header>();
		defaultHeaders.add(new BasicHeader("Connection", "keep-alive"));
		
		client = HttpClients.custom()
		           .setConnectionManager(connectionManager)
		           .setConnectionReuseStrategy(new ConnectionReuseStrategy() {
					
					@Override
					public boolean keepAlive(HttpResponse response, HttpContext context) {
						return true;
					}
				})
				.setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
					
					@Override
					public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
						return clientConfig.getIdleTimeout();
					}
				})
				.setDefaultHeaders(defaultHeaders)
				.build();
	}
	
	public void executeGet(URI uri, HttpRequestCallback callback) {
		try {
			executeInner(new HttpGet(uri), new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executePut(URI uri, HttpRequestCallback callback) {
		try {
			executeInner(new HttpPut(uri), new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executePut(URI uri, byte[] bytes, HttpRequestCallback callback) {
		try {
			HttpPut put = new HttpPut(uri);
			put.setEntity(new ByteArrayEntity(bytes));
			
			executeInner(put, new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executePost(URI uri, HttpRequestCallback callback) {
		try {
			executeInner(new HttpPost(uri), new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executePost(URI uri, byte[] bytes, HttpRequestCallback callback) {
		try {
			HttpPost post = new HttpPost(uri);
			post.setEntity(new ByteArrayEntity(bytes));
			
			executeInner(post, new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executeClose(URI uri, HttpRequestCallback callback) {
		try {
			executeInner(new HttpClose(uri), new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executeClose(URI uri, byte[] bytes, HttpRequestCallback callback) {
		try {
			HttpClose close = new HttpClose(uri);
			close.setEntity(new ByteArrayEntity(bytes));
			
			executeInner(close, new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	public void executeDelete(URI uri, HttpRequestCallback callback) {
		try {
			executeInner(new HttpDelete(uri), new UniformResponseHandler(callback));
		} catch (Exception e) {
			callback.requestFailed(e);
		}
	}
	
	private <T> void executeInner(HttpUriRequest request, ResponseHandler<T> handler) throws ClientProtocolException, IOException {
		client.execute(request, handler, null);
	}
	
	private static class UniformResponseHandler implements ResponseHandler<Void> {
		private HttpRequestCallback callback;
		
		public UniformResponseHandler(HttpRequestCallback callback) {
			this.callback = callback;
		}

		@Override
		public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
			InputStream contentInput = null;
			int contentLength = 0;
			
			try {
				HttpEntity entity = response.getEntity();
				if(entity != null) {
					contentInput = entity.getContent();
					contentLength = (int) (entity.getContentLength() > 0 ? entity.getContentLength() : 0);
				}
				
				StatusLine status = response.getStatusLine();
				if(status.getStatusCode() != HttpStatus.SC_OK) {
					callback.responseError(status.getStatusCode(), status.getReasonPhrase());
					return null;
				}
				
				ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream(contentLength);
				if(contentInput != null) {
					entity.writeTo(bytesOutput);
				}
				
				callback.responseOk(bytesOutput.toByteArray());
			} finally {
				CloseUtils.closeQuietly(contentInput);
			}
			
			return null;
		}
		
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
