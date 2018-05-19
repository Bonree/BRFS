package com.bonree.brfs.client.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.DiskServiceSelectorCache;
import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.http.client.HttpClient;
import com.bonree.brfs.common.http.client.HttpResponse;
import com.bonree.brfs.common.http.client.URIBuilder;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;

public class DefaultBRFileSystem implements BRFileSystem {
	private static final String URI_STORAGE_NAME_ROOT = "/storageName/";
	
	private static final String DEFAULT_SCHEME = "http";
	
	private HttpClient client = new HttpClient();
	private CuratorFramework zkClient;
	private ServiceManager serviceManager;
	
	private ServiceSelectorManager serviceSelectorManager;

	public DefaultBRFileSystem(String zkAddresses, String cluster) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		zkClient = CuratorFrameworkFactory.newClient(zkAddresses, 3000, 15000, retryPolicy);
		zkClient.start();
		zkClient.blockUntilConnected();
		
		ZookeeperPaths zkPaths = ZookeeperPaths.create(cluster, zkAddresses);
		serviceManager = new DefaultServiceManager(zkClient.usingNamespace(zkPaths.getBaseClusterName().substring(1)));
		serviceManager.start();
		
		this.serviceSelectorManager = new ServiceSelectorManager(serviceManager,
				zkClient, zkPaths.getBaseServerIdPath(), zkPaths.getBaseRoutePath());
	}

	@Override
	public boolean createStorageName(String storageName, Map<String, Object> attrs){
		Service service;
		try {
			service = serviceSelectorManager.useDuplicaSelector().randomService();
		} catch (Exception e1) {
			return false;
		}
		
		URIBuilder uriBuilder = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_STORAGE_NAME_ROOT + storageName);
		
		for(Entry<String, Object> attr : attrs.entrySet()) {
			uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
		}

		try {
			HttpResponse response = client.executePut(uriBuilder.build());
			String code=new String(response.getResponseBody());
			System.out.println(code);
			ReturnCode returnCode = ReturnCode.valueOf(code);
			returnCode = ReturnCode.checkCode(storageName, returnCode);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean updateStorageName(String storageName,
			Map<String, Object> attrs) {
		Service service;
		try {
			service = serviceSelectorManager.useDuplicaSelector().randomService();
		} catch (Exception e1) {
			return false;
		}
		
		URIBuilder uriBuilder = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_STORAGE_NAME_ROOT + storageName);
		
		for(Entry<String, Object> attr : attrs.entrySet()) {
			uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
		}

		try {
			HttpResponse response = client.executePost(uriBuilder.build());
			ReturnCode returnCode = ReturnCode.valueOf(new String(response.getResponseBody()));
            ReturnCode.checkCode(storageName, returnCode);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean deleteStorageName(String storageName) {
		Service service;
		try {
			service = serviceSelectorManager.useDuplicaSelector().randomService();
		} catch (Exception e1) {
			return false;
		}
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .build();

		try {
			HttpResponse response = client.executeDelete(uri);
			ReturnCode returnCode = ReturnCode.valueOf(new String(response.getResponseBody()));
			ReturnCode.checkCode(storageName, returnCode);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public StorageNameStick openStorageName(String storageName, boolean createIfNonexistent) {
		Service service;
		try {
			service = serviceSelectorManager.useDuplicaSelector().randomService();
		} catch (Exception e1) {
			return null;
		}
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_STORAGE_NAME_ROOT + storageName)
	    .build();

		try {
			HttpResponse response = client.executeGet(uri);

			int storageId = Integer.parseInt(BrStringUtils.fromUtf8Bytes(response.getResponseBody()));
			System.out.println("get id---" + storageId);
			DiskServiceSelectorCache cache = serviceSelectorManager.useDiskSelector(storageId);
			return new DefaultStorageNameStick(storageName, storageId, cache, serviceSelectorManager.useDuplicaSelector());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void close() throws IOException {
		CloseUtils.closeQuietly(client);
		CloseUtils.closeQuietly(zkClient);
		CloseUtils.closeQuietly(serviceSelectorManager);
		try {
			if(serviceManager != null) {
				serviceManager.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
