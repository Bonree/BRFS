package com.bonree.brfs.client.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.ServiceSelectorCache;
import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.http.client.HttpClient;
import com.bonree.brfs.common.http.client.HttpResponse;
import com.bonree.brfs.common.http.client.URIBuilder;
import com.bonree.brfs.common.utils.CloseUtils;
import com.google.common.primitives.Ints;

public class DefaultBRFileSystem implements BRFileSystem {
	private static final String URI_STORAGE_NAME_ROOT = "/storageName/";
	
	private static final String DEFAULT_SCHEME = "http";

	private String host;
	private int port;
	
	private HttpClient client = new HttpClient();
	
	private ServiceSelectorManager serviceSelectorManager;

	public DefaultBRFileSystem(String host, int port, ServiceSelectorManager serviceSelectorManager) {
		this.host = host;
		this.port = port;
		this.serviceSelectorManager = serviceSelectorManager;
	}

	@Override
	public boolean createStorageName(String storageName, Map<String, Object> attrs) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .addParameter("replicas", String.valueOf(attrs.get("replicas")))
	    .addParameter("ttl", String.valueOf(attrs.get("ttl")))
	    .build();

		try {
			HttpResponse response = client.executePut(uri);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean updateStorageName(String storageName,
			Map<String, Object> attrs) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .addParameter("replicas", String.valueOf(attrs.get("replicas")))
	    .addParameter("ttl", String.valueOf(attrs.get("ttl")))
	    .build();

		try {
			HttpResponse response = client.executePost(uri);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean deleteStorageName(String storageName) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .build();

		try {
			HttpResponse response = client.executeDelete(uri);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public StorageNameStick openStorageName(String storageName, boolean createIfNonexistent) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .build();

		try {
			HttpResponse response = client.executeGet(uri);

			int storageId = Ints.fromByteArray(response.getResponseBody());
			ServiceSelectorCache cache = serviceSelectorManager.useStorageIndex(storageId);
			return new DefaultStorageNameStick(storageName, storageId, cache);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void close() throws IOException {
		CloseUtils.closeQuietly(client);
	}

}
