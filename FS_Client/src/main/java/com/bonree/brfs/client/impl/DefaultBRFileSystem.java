package com.bonree.brfs.client.impl;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.DiskServiceSelectorCache;
import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.BRFSException;
import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.HttpClient;
import com.bonree.brfs.common.net.http.client.HttpResponse;
import com.bonree.brfs.common.net.http.client.URIBuilder;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;

public class DefaultBRFileSystem implements BRFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBRFileSystem.class);
    
    private HttpClient httpClient; 
    private CuratorFramework zkClient;

    private ServiceSelectorManager serviceSelectorManager;
    
    private Map<String, StorageNameStick> stickContainer = new HashMap<String, StorageNameStick>();
    
    private FileSystemConfig config;
    
    private Map<String, String> defaultHeaders = new HashMap<String, String>();

    public DefaultBRFileSystem(FileSystemConfig config) throws Exception {
    	this.config = config;
        this.httpClient = new HttpClient(ClientConfig.builder()
        		.setMaxConnection(config.getConnectionPoolSize())
        		.setMaxConnectionPerRoute(config.getConnectionPoolSize())
        		.build());
        
        this.defaultHeaders.put("username", config.getName());
        this.defaultHeaders.put("password", config.getPasswd());
    	
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(config.getZkAddresses(), 3000, 15000, retryPolicy);
        zkClient.start();
        zkClient.blockUntilConnected();
        
        ZookeeperPaths zkPaths = ZookeeperPaths.getBasePath(config.getClusterName(), config.getZkAddresses());
        if(zkClient.checkExists().forPath(zkPaths.getBaseClusterName()) == null) {
            throw new BRFSException("cluster is not exist!!!");
        }
        
        this.serviceSelectorManager = new ServiceSelectorManager(zkClient, zkPaths.getBaseClusterName().substring(1), zkPaths.getBaseServerIdPath(), zkPaths.getBaseRoutePath(),
        		config.getDuplicateServiceGroup(), config.getDiskServiceGroup());
    }

    @Override
    public boolean createStorageName(String storageName, Map<String, Object> attrs) {
        try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	if(serviceList.isEmpty()) {
        		throw new BRFSException("none disknode!!!");
        	}
        	
        	for(Service service : serviceList) {
        		URIBuilder uriBuilder = new URIBuilder().setScheme(config.getUrlSchema())
        				.setHost(service.getHost()).setPort(service.getPort())
        				.setPath(config.getStorageUrlRoot() + "/" + storageName);

                for (Entry<String, Object> attr : attrs.entrySet()) {
                    uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
                }
            	
                HttpResponse response = null;
            	try {
            		 response = httpClient.executePut(uriBuilder.build(), defaultHeaders);
    			} catch (Exception e) {
    				LOG.warn("createStorageName http request failed", e);
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for createStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
                ReturnCode returnCode = ReturnCode.checkCode(storageName, code);
        	}
        } catch (Exception e) {
            LOG.error("createStorageName error", e);
        }

        return false;
    }

    @Override
    public boolean updateStorageName(String storageName, Map<String, Object> attrs) {
    	try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	if(serviceList.isEmpty()) {
        		throw new BRFSException("none disknode!!!");
        	}
        	
        	for(Service service : serviceList) {
        		URIBuilder uriBuilder = new URIBuilder().setScheme(config.getUrlSchema())
        				.setHost(service.getHost()).setPort(service.getPort())
        				.setPath(config.getStorageUrlRoot() + "/" + storageName);

                for (Entry<String, Object> attr : attrs.entrySet()) {
                    uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
                }
            	
                HttpResponse response = null;
            	try {
            		Map<String, String> headers = new HashMap<String, String>();
            		headers.put("username", config.getName());
            		headers.put("password", config.getPasswd());
            		
            		response = httpClient.executePost(uriBuilder.build(), headers);
    			} catch (Exception e) {
    				LOG.warn("updateStorageName http request failed", e);
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for updateStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
        		ReturnCode returnCode = ReturnCode.checkCode(storageName, code);
        	}
        } catch (Exception e) {
        	LOG.error("updateStorageName error", e);
        }
    	
    	return false;
    }

    @Override
    public boolean deleteStorageName(String storageName) {
    	try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	if(serviceList.isEmpty()) {
        		throw new BRFSException("none disknode!!!");
        	}
        	
        	for(Service service : serviceList) {
        		URI uri = new URIBuilder().setScheme(config.getUrlSchema())
        				.setHost(service.getHost()).setPort(service.getPort())
        				.setPath(config.getStorageUrlRoot() + "/" + storageName).build();
            	
                HttpResponse response = null;
            	try {
            		response = httpClient.executeDelete(uri, defaultHeaders);
    			} catch (Exception e) {
    				LOG.warn("deleteStorageName http request failed", e);
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for deleteStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
        		ReturnCode returnCode = ReturnCode.checkCode(storageName, code);
        	}
        } catch (Exception e) {
        	LOG.error("deleteStorageName error", e);
        }

        return false;
    }

    @Override
    public StorageNameStick openStorageName(String storageName) {
    	StorageNameStick stick = stickContainer.get(storageName);
    	if(stick == null) {
    		synchronized (stickContainer) {
    			stick = stickContainer.get(storageName);
    			if(stick == null) {
    				try {
    		        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
    		        	if(serviceList.isEmpty()) {
    		        		throw new BRFSException("none disknode!!!");
    		        	}
    		        	
    		        	for(Service service : serviceList) {
    		        		URI uri = new URIBuilder().setScheme(config.getUrlSchema())
    		        				.setHost(service.getHost()).setPort(service.getPort())
    		        				.setPath(config.getStorageUrlRoot() + "/" + storageName).build();
    		            	
    		                HttpResponse response = null;
    		            	try {
    		            		response = httpClient.executeGet(uri, defaultHeaders);
    		    			} catch (Exception e) {
    		    				LOG.warn("openStorageName http request failed", e);
    		    				continue;
    		    			}
    		            	
    		            	if(response == null) {
    		            		throw new Exception("can not get response for deleteStorageName!");
    		            	}
    		            	
    		            	if(response.isReponseOK()) {
    		            		int storageId = Integer.parseInt(BrStringUtils.fromUtf8Bytes(response.getResponseBody()));
    		            		
    		            		DiskServiceSelectorCache cache = serviceSelectorManager.useDiskSelector(storageId);
    	    		            stick = new DefaultStorageNameStick(storageName, storageId,
    	    		            		httpClient, cache, serviceSelectorManager.useDuplicaSelector(),
    	    		            		config);
    	    		            stickContainer.put(storageName, stick);
    	    		            
    	    		            return stick;
    		        		}
    		        		
    		        		String code = new String(response.getResponseBody());
    		        		ReturnCode returnCode = ReturnCode.checkCode(storageName, code);
    		        	}
    		        } catch (Exception e) {
    		        	LOG.error("openStorageName error", e);
    		        }
    			}
			}
    	}

        return stick;
    }

    @Override
    public void close() throws IOException {
        for(StorageNameStick stick : stickContainer.values()) {
        	stick.close();
        }
        
        CloseUtils.closeQuietly(serviceSelectorManager);
        CloseUtils.closeQuietly(zkClient);
        CloseUtils.closeQuietly(httpClient);
    }

}
