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
import com.bonree.brfs.common.http.client.HttpClient;
import com.bonree.brfs.common.http.client.HttpResponse;
import com.bonree.brfs.common.http.client.URIBuilder;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;

public class DefaultBRFileSystem implements BRFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBRFileSystem.class);
    private static final String URI_STORAGE_NAME_ROOT = "/storageName/";

    private static final String DEFAULT_SCHEME = "http";

    private HttpClient client = new HttpClient();
    private CuratorFramework zkClient;
    private ServiceManager serviceManager;

    private ServiceSelectorManager serviceSelectorManager;
    
    private Map<String, StorageNameStick> stickContainer = new HashMap<String, StorageNameStick>();

    public DefaultBRFileSystem(String zkAddresses, String cluster) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkAddresses, 3000, 15000, retryPolicy);
        zkClient.start();
        zkClient.blockUntilConnected();
        ZookeeperPaths zkPaths = ZookeeperPaths.getBasePath(cluster, zkAddresses);
        if(zkClient.checkExists().forPath(zkPaths.getBaseClusterName()) == null) {
            throw new BRFSException("cluster is not exist!!!");
        }
        serviceManager = new DefaultServiceManager(zkClient.usingNamespace(zkPaths.getBaseClusterName().substring(1)));
        serviceManager.start();
        this.serviceSelectorManager = new ServiceSelectorManager(serviceManager, zkClient, zkPaths.getBaseServerIdPath(), zkPaths.getBaseRoutePath());
    }

    @Override
    public boolean createStorageName(String storageName, Map<String, Object> attrs) {
        try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	for(Service service : serviceList) {
        		URIBuilder uriBuilder = new URIBuilder().setScheme(DEFAULT_SCHEME).setHost(service.getHost()).setPort(service.getPort()).setPath(URI_STORAGE_NAME_ROOT + storageName);

                for (Entry<String, Object> attr : attrs.entrySet()) {
                    uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
                }
            	
                HttpResponse response = null;
            	try {
            		 response = client.executePut(uriBuilder.build());
    			} catch (Exception e) {
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for createStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
                ReturnCode returnCode = ReturnCode.valueOf(code);
                returnCode = ReturnCode.checkCode(storageName, returnCode);
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean updateStorageName(String storageName, Map<String, Object> attrs) {
    	try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	for(Service service : serviceList) {
        		URIBuilder uriBuilder = new URIBuilder().setScheme(DEFAULT_SCHEME).setHost(service.getHost()).setPort(service.getPort()).setPath(URI_STORAGE_NAME_ROOT + storageName);

                for (Entry<String, Object> attr : attrs.entrySet()) {
                    uriBuilder.addParameter(attr.getKey(), String.valueOf(attr.getValue()));
                }
            	
                HttpResponse response = null;
            	try {
            		response = client.executePost(uriBuilder.build());
    			} catch (Exception e) {
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for updateStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
                ReturnCode returnCode = ReturnCode.valueOf(code);
                returnCode = ReturnCode.checkCode(storageName, returnCode);
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
    	return false;
    }

    @Override
    public boolean deleteStorageName(String storageName) {
    	try {
        	List<Service> serviceList = serviceSelectorManager.useDuplicaSelector().randomServiceList();
        	for(Service service : serviceList) {
        		URI uri = new URIBuilder().setScheme(DEFAULT_SCHEME).setHost(service.getHost()).setPort(service.getPort()).setPath(URI_STORAGE_NAME_ROOT + storageName).build();
            	
                HttpResponse response = null;
            	try {
            		response = client.executeDelete(uri);
    			} catch (Exception e) {
    				continue;
    			}
            	
            	if(response == null) {
            		throw new Exception("can not get response for deleteStorageName!");
            	}
            	
            	if(response.isReponseOK()) {
        			return true;
        		}
        		
        		String code = new String(response.getResponseBody());
                ReturnCode returnCode = ReturnCode.valueOf(code);
                returnCode = ReturnCode.checkCode(storageName, returnCode);
        	}
        } catch (Exception e) {
            e.printStackTrace();
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
    		        	for(Service service : serviceList) {
    		        		URI uri = new URIBuilder().setScheme(DEFAULT_SCHEME).setHost(service.getHost()).setPort(service.getPort()).setPath(URI_STORAGE_NAME_ROOT + storageName).build();
    		            	
    		                HttpResponse response = null;
    		            	try {
    		            		response = client.executeGet(uri);
    		    			} catch (Exception e) {
    		    				continue;
    		    			}
    		            	
    		            	if(response == null) {
    		            		throw new Exception("can not get response for deleteStorageName!");
    		            	}
    		            	
    		            	if(response.isReponseOK()) {
    		            		int storageId = Integer.parseInt(BrStringUtils.fromUtf8Bytes(response.getResponseBody()));
    		            		
    		            		DiskServiceSelectorCache cache = serviceSelectorManager.useDiskSelector(storageId);
    	    		            stick = new DefaultStorageNameStick(storageName, storageId, client, cache, serviceSelectorManager.useDuplicaSelector());
    	    		            stickContainer.put(storageName, stick);
    	    		            
    	    		            return stick;
    		        		}
    		        		
    		        		String code = new String(response.getResponseBody());
    		                ReturnCode returnCode = ReturnCode.valueOf(code);
    		                returnCode = ReturnCode.checkCode(storageName, returnCode);
    		        	}
    		        } catch (Exception e) {
    		            e.printStackTrace();
    		        }
    			}
			}
    	}

        return stick;
    }

    @Override
    public void close() throws IOException {
        CloseUtils.closeQuietly(client);
        CloseUtils.closeQuietly(zkClient);
        CloseUtils.closeQuietly(serviceSelectorManager);
        
        for(StorageNameStick stick : stickContainer.values()) {
        	CloseUtils.closeQuietly(stick);
        }
        
        try {
            if (serviceManager != null) {
                serviceManager.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
