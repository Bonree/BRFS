package com.bonree.brfs.duplication.rocksdb.client.impl;

import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.HttpClient;
import com.bonree.brfs.common.net.http.client.HttpResponse;
import com.bonree.brfs.common.net.http.client.URIBuilder;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.duplication.rocksdb.client.RegionNodeClient;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 11:24
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class HttpRegionNodeClient implements RegionNodeClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRegionNodeClient.class);

    private static final String DEFAULT_SCHEME = "http";
    private static final String URI_PATH_ROCKSDB_ROOT = "/rocksdb/";

    private HttpClient client;

    private String host;
    private int port;

    public HttpRegionNodeClient(String host, int port) {
        this(host, port, ClientConfig.DEFAULT);
    }

    public HttpRegionNodeClient(String host, int port, ClientConfig clientConfig) {
        this.host = host;
        this.port = port;
        this.client = new HttpClient(clientConfig);
    }

    @Override
    public boolean ping() {
        URI uri = new URIBuilder()
                .setScheme(DEFAULT_SCHEME)
                .setHost(host)
                .setPort(port)
                .setPath("/ping/")
                .build();

        try {
            HttpResponse response = client.executeGet(uri);
            return response.isReponseOK();
        } catch (Exception e) {
            LOG.error("region node ping to {}:{} error", host, port, e);
        }

        return false;
    }

    @Override
    public byte[] readData(RocksDBDataUnit unit) {
        // 暂不实现 todo
        return new byte[0];
    }

    @Override
    public void writeData(RocksDBDataUnit unit) throws IOException {

        URI uri = new URIBuilder()
                .setScheme(DEFAULT_SCHEME)
                .setHost(host)
                .setPort(port)
                .setPath(URI_PATH_ROCKSDB_ROOT)
                .build();

        try {
            LOG.info("write rocksdb data to {}:{}, cf: {}", host, port, unit.getColumnFamily());
            HttpResponse response = client.executePost(uri, ProtoStuffUtils.serialize(unit));
            LOG.debug("write rocksdb response[{}], host:{}, port:{}", host, port, response.getStatusCode());
        } catch (Exception e) {
            LOG.error("write rocksdb data to {}:{} error", host, port, e);
        }
    }

    @Override
    public List<Integer> restoreData(String fileName, String restorePath, String host, int port) {

        URI uri = new URIBuilder()
                .setScheme(DEFAULT_SCHEME)
                .setHost(this.host)
                .setPort(this.port)
                .setParamter("transferFileName", fileName)
                .setParamter("restorePath", restorePath)
                .setParamter("host", host)
                .setParamter("port", String.valueOf(port))
                .setPath("/ping/")
                .build();

        try {
            LOG.info("send restore request to {}:{}, transferFileName:{}, restorePath:{}", this.host, this.port, fileName, restorePath);
            HttpResponse response = client.executePost(uri);
            LOG.debug("restore request {}:{} response[{}]", this.host, this.port, response.getStatusCode());

            if (response.isReponseOK()) {
                return JsonUtils.toObject(response.getResponseBody(), new TypeReference<List<Integer>>() {
                });
            }
        } catch (Exception e) {
            LOG.error("send restore request to {}:{} error", this.host, this.port, e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
