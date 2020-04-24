package com.bonree.brfs.client.impl;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.ServiceSelectorManager;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.BRFSException;
import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.HttpClient;
import com.bonree.brfs.common.net.http.client.HttpResponse;
import com.bonree.brfs.common.net.http.client.URIBuilder;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBRFileSystem implements BRFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBRFileSystem.class);

    private HttpClient httpClient;
    private CuratorFramework zkClient;

    private ServiceManager serviceManager;
    private ServiceSelectorManager serviceSelectorManager;
    private RegionNodeSelector regionNodeSelector;

    private FileSystemConfig config;

    private Map<String, String> defaultHeaders = new HashMap<String, String>();

    public DefaultBRFileSystem(FileSystemConfig config) throws Exception {
        this.config = config;
        this.httpClient = new HttpClient(ClientConfig.builder()
                                                     .setMaxConnection(config.getConnectionPoolSize())
                                                     .setMaxConnectionPerRoute(config.getConnectionPoolSize())
                                                     .setIOThreadNum(config.getHandleThreadNum())
                                                     .setResponseTimeout(60 * 1000)
                                                     .build());

        this.defaultHeaders.put("username", config.getName());
        this.defaultHeaders.put("password", config.getPasswd());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(config.getZkAddresses(), 3000, 15000, retryPolicy);
        zkClient.start();
        zkClient.blockUntilConnected(config.getZkConnectTimeoutSeconds(), TimeUnit.SECONDS);

        ZookeeperPaths zkPaths = ZookeeperPaths.getBasePath(config.getClusterName(), zkClient);
        if (zkClient.checkExists().forPath(zkPaths.getBaseClusterName()) == null) {
            throw new BRFSException("cluster is not exist!!!");
        }

        this.serviceManager = new DefaultServiceManager(zkClient, zkPaths);
        this.serviceManager.start();

        this.serviceSelectorManager = new ServiceSelectorManager(zkClient, zkPaths.getBaseClusterName().substring(1),
                                                                 zkPaths.getBaseServerIdPath(), zkPaths.getBaseRoutePath(),
                                                                 serviceManager, config.getDiskServiceGroup());

        this.regionNodeSelector = new RegionNodeSelector(serviceManager, config.getDuplicateServiceGroup());
    }

    @Override
    public boolean createStorageName(String storageName, Map<String, Object> attrs) {
        try {
            Service[] serviceList = regionNodeSelector.select(regionNodeSelector.serviceNum());
            if (serviceList.length == 0) {
                throw new BRFSException("none disknode!!!");
            }

            for (Service service : serviceList) {
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

                if (response == null) {
                    throw new Exception("can not get response for createStorageName!");
                }

                if (response.isReponseOK()) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.error("createStorageName error", e);
        }

        return false;
    }

    @Override
    public boolean updateStorageName(String storageName, Map<String, Object> attrs) {
        try {
            Service[] serviceList = regionNodeSelector.select(regionNodeSelector.serviceNum());
            if (serviceList.length == 0) {
                throw new BRFSException("none disknode!!!");
            }

            for (Service service : serviceList) {
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

                if (response == null) {
                    throw new Exception("can not get response for updateStorageName!");
                }

                if (response.isReponseOK()) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.error("updateStorageName error", e);
        }

        return false;
    }

    @Override
    public boolean deleteStorageName(String storageName) {
        try {
            Service[] serviceList = regionNodeSelector.select(regionNodeSelector.serviceNum());
            if (serviceList.length == 0) {
                throw new BRFSException("none disknode!!!");
            }

            for (Service service : serviceList) {
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

                if (response == null) {
                    throw new Exception("can not get response for deleteStorageName!");
                }

                if (response.isReponseOK()) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.error("deleteStorageName error", e);
        }

        return false;
    }

    @Override
    public StorageNameStick openStorageName(String storageName) {
        StorageNameStick stick = null;
        try {
            Service[] serviceList = regionNodeSelector.select(regionNodeSelector.serviceNum());
            if (serviceList.length == 0) {
                throw new BRFSException("none disknode!!!");
            }

            for (Service service : serviceList) {
                URI uri = new URIBuilder()
                    .setScheme(config.getUrlSchema())
                    .setHost(service.getHost())
                    .setPort(service.getPort())
                    .setPath(config.getStorageUrlRoot() + "/" + storageName)
                    .build();

                HttpResponse response = null;
                try {
                    response = httpClient.executeGet(uri, defaultHeaders);
                } catch (Exception e) {
                    LOG.warn("openStorageName http request failed", e);
                    continue;
                }

                if (response == null) {
                    throw new Exception(
                        "can not get response for deleteStorageName!");
                }

                if (response.isReponseOK()) {
                    int storageId = Ints.fromByteArray(response
                                                           .getResponseBody());

                    stick = new DefaultStorageNameStick(storageName, storageId,
                                                        httpClient,
                                                        serviceSelectorManager.useDiskSelector(storageId),
                                                        regionNodeSelector, config);

                    return stick;
                }
            }
        } catch (Exception e) {
            LOG.error("openStorageName error", e);
        }

        return stick;
    }

    @Override
    public void close() throws IOException {
        CloseUtils.closeQuietly(serviceSelectorManager);
        CloseUtils.closeQuietly(zkClient);
        CloseUtils.closeQuietly(httpClient);
    }

}
