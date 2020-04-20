/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bonree.brfs.client;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;

import com.bonree.brfs.client.data.read.FidContentReader;
import com.bonree.brfs.client.data.read.FilePathMapper;
import com.bonree.brfs.client.data.read.HttpFilePathMapper;
import com.bonree.brfs.client.data.read.PooledTcpFidContentReader;
import com.bonree.brfs.client.data.read.StringSubFidParser;
import com.bonree.brfs.client.data.read.SubFidParser;
import com.bonree.brfs.client.data.read.TcpFidContentReader;
import com.bonree.brfs.client.data.read.connection.DataConnectionPool;
import com.bonree.brfs.client.discovery.CachedDiscovery;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.HttpDiscovery;
import com.bonree.brfs.client.discovery.NodeSelector;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.ranker.ShiftRanker;
import com.bonree.brfs.client.route.HttpRouterClient;
import com.bonree.brfs.client.route.Router;
import com.bonree.brfs.client.route.RouterClient;
import com.bonree.brfs.client.utils.DaemonThreadFactory;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;

import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Response;

public class BRFSClientBuilder {
    private ClientConfiguration configuration;
    
    public BRFSClientBuilder config(ClientConfiguration config) {
        this.configuration = config;
        return this;
    }
    
    /**
     * create a brfs client with a customized configuration
     * 
     * @param user user for brfs server
     * @param passwd the secret key of user
     * @param regionNodes addresses of region nodes. get the seed addresses for service discovery, It's not
     *                    necessary to provide all addresses of region nodes in
     *                    cluster. perhaps one is just enough.
     *                    
     * @return brfs client
     */
    public BRFS build(String user, String passwd, URI[] regionNodes) {
        if(configuration == null) {
            configuration = new ClientConfigurationBuilder().build();
        }
        
        Closer closer = Closer.create();
        
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addNetworkInterceptor(new AuthorizationIterceptor(user, passwd))
                .socketFactory(new SocketChannelSocketFactory())
                .callTimeout(configuration.getRequestTimeout())
                .connectTimeout(configuration.getConnectTimeout())
                .readTimeout(configuration.getReadTimeout())
                .writeTimeout(configuration.getWriteTimeout())
                .build();
        closer.register(() -> {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        });
        
        JsonCodec codec = new JsonCodec(new ObjectMapper());
        
        Discovery discovery = new CachedDiscovery(
                new HttpDiscovery(httpClient, regionNodes, codec),
                Executors.newSingleThreadExecutor(new DaemonThreadFactory("brfs-discovery-%s")),
                configuration.getDiscoveryExpiredDuration(),
                configuration.getDiscoreryRefreshDuration());
        
        NodeSelector nodeSelector = new NodeSelector(discovery, new ShiftRanker<>());
        closer.register(nodeSelector);
        
        RouterClient routerClient = new HttpRouterClient(httpClient, nodeSelector, codec);
        
        DataConnectionPool pool = new DataConnectionPool();
        closer.register(pool);
        
        FidContentReader contentReader = new PooledTcpFidContentReader(pool);
        
        FilePathMapper pathMapper = new HttpFilePathMapper(httpClient, nodeSelector);
        SubFidParser subFidParser = new StringSubFidParser();
                
        return new BRFSClient(
                configuration,
                httpClient,
                nodeSelector,
                new Router(routerClient),
                contentReader,
                pathMapper,
                subFidParser,
                codec,
                closer);
    }
    
    /**
     * TODO It's dangerous to transfer plain text of password in header
     */
    public static class AuthorizationIterceptor implements Interceptor {
        private final String user;
        private final String passwd;
        
        public AuthorizationIterceptor(String user, String passwd) {
            this.user = user;
            this.passwd = passwd;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            return chain.proceed(chain.request()
                    .newBuilder()
                    .header("username", user)
                    .header("password", passwd)
                    .build());
        }
        
    }
}
