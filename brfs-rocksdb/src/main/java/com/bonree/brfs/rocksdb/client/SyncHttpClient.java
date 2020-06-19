package com.bonree.brfs.rocksdb.client;

import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.HttpResponse;
import com.bonree.brfs.common.net.http.client.HttpResponseProxy;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/25 16:32
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class SyncHttpClient implements Closeable {

    private CloseableHttpClient client;

    public SyncHttpClient() {
        this(ClientConfig.DEFAULT);
    }

    public SyncHttpClient(ClientConfig clientConfig) {

        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                                                            .setBufferSize(clientConfig.getBufferSize())
                                                            .setCharset(Consts.UTF_8)
                                                            .build();
        RequestConfig requestConfig = RequestConfig.custom()
                                                   .setConnectTimeout(clientConfig.getConnectTimeout())
                                                   .build();

        List<Header> defaultHeaders = new ArrayList<Header>();
        if (clientConfig.isKeepAlive()) {
            defaultHeaders.add(new BasicHeader("Connection", "keep-alive"));
        }

        client = HttpClientBuilder.create()
                                  .setMaxConnPerRoute(clientConfig.getMaxConnectionPerRoute())
                                  .setMaxConnTotal(clientConfig.getMaxConnection())
                                  .setDefaultConnectionConfig(connectionConfig)
                                  .setDefaultRequestConfig(requestConfig)
                                  .setConnectionReuseStrategy(new ConnectionReuseStrategy() {

                                      @Override
                                      public boolean keepAlive(org.apache.http.HttpResponse response, HttpContext context) {
                                          return clientConfig.isKeepAlive();
                                      }

                                  })
                                  .setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {

                                      @Override
                                      public long getKeepAliveDuration(org.apache.http.HttpResponse response,
                                                                       HttpContext context) {
                                          return clientConfig.getIdleTimeout();
                                      }

                                  })
                                  .setDefaultHeaders(defaultHeaders)
                                  .build();
    }

    public HttpResponse executeGet(URI uri) throws Exception {
        return executeInner(new HttpGet(uri));
    }

    public HttpResponse executeGet(URI uri, Map<String, String> headers) throws Exception {
        HttpGet httpGet = new HttpGet(uri);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            httpGet.setHeader(entry.getKey(), entry.getValue());
        }

        return executeInner(httpGet);
    }

    public HttpResponse executePut(URI uri) throws Exception {
        return executeInner(new HttpPut(uri));
    }

    public HttpResponse executePut(URI uri, Map<String, String> headers) throws Exception {
        HttpPut httpPut = new HttpPut(uri);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            httpPut.setHeader(entry.getKey(), entry.getValue());
        }

        return executeInner(httpPut);
    }

    public HttpResponse executePut(URI uri, byte[] bytes) throws Exception {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new ByteArrayEntity(bytes));

        return executeInner(put);
    }

    public HttpResponse executePut(URI uri, Map<String, String> headers, byte[] bytes) throws Exception {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new ByteArrayEntity(bytes));

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            put.setHeader(entry.getKey(), entry.getValue());
        }

        return executeInner(put);
    }

    public HttpResponse executePost(URI uri) throws Exception {
        return executeInner(new HttpPost(uri));
    }

    public HttpResponse executePost(URI uri, Map<String, String> headers) throws Exception {
        HttpPost post = new HttpPost(uri);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            post.setHeader(entry.getKey(), entry.getValue());
        }

        return executeInner(post);
    }

    public HttpResponse executePost(URI uri, byte[] bytes) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new ByteArrayEntity(bytes));

        return executeInner(post);
    }

    public HttpResponse executePost(URI uri, Map<String, String> headers, byte[] bytes) throws Exception {
        HttpPost post = new HttpPost(uri);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            post.setHeader(entry.getKey(), entry.getValue());
        }

        post.setEntity(new ByteArrayEntity(bytes));

        return executeInner(post);
    }

    private HttpResponse executeInner(HttpUriRequest request) throws Exception {
        org.apache.http.HttpResponse response = client.execute(request);
        return new HttpResponseProxy(response);
    }

    @Override
    public void close() throws IOException {
        HttpClientUtils.closeQuietly(client);
    }

}
