package com.br.disknode.client;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

import com.br.disknode.utils.InputUtils;
import com.google.common.io.Closeables;

public class HttpDiskNodeClient implements DiskNodeClient {

	private static final String URI_DISK_NODE_ROOT = "/disk";

	private static final int STATUS_OK = 200;

	private CloseableHttpClient client;

	private String host;
	private int port;

	public HttpDiskNodeClient(String host, int port) {
		this.host = host;
		this.port = port;

		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		// Increase max total connection to 200
		cm.setMaxTotal(200);
		// Increase default max connection per route to 20
		cm.setDefaultMaxPerRoute(20);
		// Increase max connections for localhost:80 to 50
		HttpHost httpHost = new HttpHost(host, 80);
		cm.setMaxPerRoute(new HttpRoute(httpHost), 50);

		client = HttpClients.custom()
				.setConnectionManager(cm)
				.setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {

					@Override
					public long getKeepAliveDuration(HttpResponse response,
							HttpContext context) {
						return 20 * 1000;
					}

				}).build();
	}

	private String buildUri(String path) {
		StringBuilder uriBuilder = new StringBuilder("http://");
		uriBuilder.append(host).append(':').append(port)
				.append(URI_DISK_NODE_ROOT).append(path);

		return uriBuilder.toString();
	}

	@Override
	public boolean initFile(String path, boolean override) {
		StringBuilder pathBuilder = new StringBuilder(path);
		pathBuilder.append("?override=").append(override);
		HttpPut httpPut = new HttpPut(buildUri(pathBuilder.toString()));

		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPut);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public void writeData(String path, byte[] bytes) throws IOException {
		writeData(path, bytes, 0, bytes.length);
	}

	@Override
	public void writeData(String path, byte[] bytes, int offset, int size)
			throws IOException {
		HttpPost httpPost = new HttpPost(buildUri(path));
		ByteArrayEntity requestEntity = new ByteArrayEntity(bytes, offset, size);
		httpPost.setEntity(requestEntity);

		try {
			CloseableHttpResponse response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() != STATUS_OK) {
				throw new IOException();
			}

			HttpEntity responseEntity = response.getEntity();
			byte[] result = new byte[(int) responseEntity.getContentLength()];
			InputUtils.readBytes(responseEntity.getContent(), bytes, 0,
					bytes.length);

			System.out.println(new String(result));
		} catch (ClientProtocolException e) {
			throw new IOException(e);
		}
	}

	@Override
	public byte[] readData(String path, int offset, int size)
			throws IOException {
		StringBuilder pathBuilder = new StringBuilder(path);
		pathBuilder.append("?offset=").append(offset).append("&size=")
				.append(size);
		HttpGet httpGet = new HttpGet(buildUri(pathBuilder.toString()));

		try {
			CloseableHttpResponse response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() != STATUS_OK) {
				throw new IOException();
			}

			HttpEntity entity = response.getEntity();
			byte[] bytes = new byte[(int) entity.getContentLength()];
			InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

			return bytes;
		} catch (ClientProtocolException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean closeFile(String path) {
		HttpClose httpClose = new HttpClose(buildUri(path));

		try {
			CloseableHttpResponse response = client.execute(httpClose);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public boolean deleteFile(String path) {
		HttpDelete httpDelete = new HttpDelete(buildUri(path));

		try {
			CloseableHttpResponse response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public boolean deleteDir(String path, boolean recursive) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getValidLength(String path) {
		// TODO Auto-generated method stub
		return 0;
	}

}
