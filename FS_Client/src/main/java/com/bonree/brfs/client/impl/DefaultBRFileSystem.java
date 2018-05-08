package com.bonree.brfs.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.ServiceSelectorCache;
import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.InputUtils;
import com.google.common.primitives.Ints;

public class DefaultBRFileSystem implements BRFileSystem {
	private static final String URI_STORAGE_NAME_ROOT = "/storageName/";

	private static final int STATUS_OK = 200;

	private String host;
	private int port;
	
	private ServiceSelectorManager serviceSelectorManager;

	public DefaultBRFileSystem(String host, int port, ServiceSelectorManager serviceSelectorManager) {
		this.host = host;
		this.port = port;
		this.serviceSelectorManager = serviceSelectorManager;
	}
	
	private URI buildUri(String root, String path, Map<String, String> params) {
		List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
		for(String name : params.keySet()) {
			nameValuePairs.add(new BasicNameValuePair(name, params.get(name)));
		}
		
		try {
			return new URIBuilder()
			.setScheme("http")
			.setHost(host)
			.setPort(port)
			.setPath(root + path)
			.setParameters(nameValuePairs)
			.build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public boolean createStorageName(String storageName,
			Map<String, Object> attrs) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("replicas", String.valueOf(attrs.get("replicas")));
		params.put("ttl", String.valueOf(attrs.get("ttl")));
		HttpPut httpPut = new HttpPut(buildUri(URI_STORAGE_NAME_ROOT, storageName, params));

		CloseableHttpClient client = HttpClients.createDefault();
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
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}

		return false;
	}

	@Override
	public boolean updateStorageName(String storageName,
			Map<String, Object> attrs) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("replicas", String.valueOf(attrs.get("replicas")));
		params.put("ttl", String.valueOf(attrs.get("ttl")));
		HttpPost httpPost = new HttpPost(buildUri(URI_STORAGE_NAME_ROOT, storageName, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}

		return false;
	}

	@Override
	public boolean deleteStorageName(String storageName) {
		Map<String, String> params = new HashMap<String, String>();
		HttpDelete httpDelete = new HttpDelete(buildUri(URI_STORAGE_NAME_ROOT, storageName, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}

		return false;
	}

	@Override
	public StorageNameStick openStorageName(String storageName,
			boolean createIfNonexistent) {
		Map<String, String> params = new HashMap<String, String>();
		HttpGet httpDelete = new HttpGet(buildUri(URI_STORAGE_NAME_ROOT, storageName, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			if(status.getStatusCode() == STATUS_OK) {
				HttpEntity responseEntity = response.getEntity();
				byte[] resultBytes = new byte[(int) responseEntity.getContentLength()];
				InputUtils.readBytes(responseEntity.getContent(), resultBytes, 0, resultBytes.length);

				int storageId = Ints.fromByteArray(resultBytes);
				ServiceSelectorCache cache = serviceSelectorManager.useStorageIndex(storageId);
				return new DefaultStorageNameStick(storageId, cache);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}

		return null;
	}

}
