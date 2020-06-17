package com.bonree.brfs.rocksdb.client.impl;

import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.URIBuilder;
import com.bonree.brfs.rocksdb.client.RegionNodeClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String URI_PATH_INNER_READ = "/rocksdb/inner/read/";
    private static final String URI_PATH_INNER_WRITE = "/rocksdb/inner/write/";
    private static final String URI_PATH_RESTORE = "/rocksdb/inner/restore/";

    private OkHttpClient client;

    private static final Duration DEFAULT_CONNECTION_TIME_OUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_REQUEST_TIME_OUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_READ_TIME_OUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_WRITE_TIME_OUT = Duration.ofSeconds(30);

    private String host;
    private int port;

    public HttpRegionNodeClient(String host, int port) {
        this(host, port, ClientConfig.DEFAULT);
    }

    public HttpRegionNodeClient(String host, int port, ClientConfig clientConfig) {
        this.host = host;
        this.port = port;
        this.client = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .callTimeout(DEFAULT_REQUEST_TIME_OUT)
            .connectTimeout(DEFAULT_CONNECTION_TIME_OUT)
            .readTimeout(DEFAULT_READ_TIME_OUT)
            .writeTimeout(DEFAULT_WRITE_TIME_OUT)
            .build();
    }

    @Override
    public boolean ping() {
        return false;
    }

    @Override
    public byte[] readData(String columnFamily, String key) {
        URI uri = new URIBuilder()
            .setScheme(DEFAULT_SCHEME)
            .setHost(host)
            .setPort(port).build();

        Request httpRequest = new Request.Builder()
            .url(Objects.requireNonNull(HttpUrl.get(uri))
                        .newBuilder()
                        .encodedPath(URI_PATH_INNER_READ)
                        .addEncodedQueryParameter("srName", columnFamily)
                        .addEncodedQueryParameter("fileName", key)
                        .build())
            .build();

        try {
            LOG.info("read rocksdb data from {}, cf: {}, key:{}", host, columnFamily, key);
            Response response = client.newCall(httpRequest).execute();
            if (response.isSuccessful()) {
                return response.body().bytes();
            }
            LOG.debug("read rocksdb response[{}], host:{}, port:{}, cf: {}, key:{}", response.code(), host, port,
                      columnFamily, key);
        } catch (Exception e) {
            LOG.error("read rocksdb data to {}:{} error, cf: {}, key:{}", host, port, columnFamily, key, e);
            return null;
        }
        return null;
    }

    @Override
    public void writeData(String columnFamily, String key, String value) {

        URI uri = new URIBuilder()
            .setScheme(DEFAULT_SCHEME)
            .setHost(host)
            .setPort(port)
            .build();

        Request httpRequest = new Request.Builder()
            .url(Objects.requireNonNull(HttpUrl.get(uri))
                        .newBuilder()
                        .encodedPath(URI_PATH_INNER_WRITE)
                        .addEncodedQueryParameter("cf", columnFamily)
                        .addEncodedQueryParameter("key", key)
                        .addEncodedQueryParameter("value", value)
                        .build())
            .build();

        try {
            LOG.info("write rocksdb data to {}:{}, cf: {}, key:{}, value:{}", host, port, columnFamily, key, value);
            client.newCall(httpRequest).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    LOG.error("write rocksdb data to {}:{} error, cf: {}, key:{}, value:{}", host, port, columnFamily, key,
                              value);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    LOG.info("write rocksdb response[{}] from {}:{}, cf: {}, key:{}, value:{}", response.code(), host, port,
                             columnFamily, key, value);
                }
            });
        } catch (Exception e) {
            LOG.error("write rocksdb data to {}:{} error, cf: {}, key:{}, value:{}", host, port, columnFamily, key, value, e);
        }
    }

    @Override
    public List<Integer> restoreData(String fileName, String restorePath, String host, int port) {
        return null;
    }

    @Override
    public void close() throws IOException {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }
}
