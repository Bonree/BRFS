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
package com.bonree.brfs.duplication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Singleton;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.Lifecycle.LifeCycleObject;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.net.http.HttpConfig;
import com.bonree.brfs.common.net.http.netty.HttpAuthenticator;
import com.bonree.brfs.common.net.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.net.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManager;
import com.bonree.brfs.duplication.datastream.blockcache.BlockPool;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteStreamDataMessageHandler;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

public class LegacyNettyServerModule implements Module {
    
    @Override
    public void configure(Binder binder) {
        LifecycleModule.register(binder, NettyHttpServer.class);
    }

    @Provides
    @Singleton
    public NettyHttpServer getHttpServer(
            SimpleAuthentication simpleAuthentication,
            ZookeeperPaths paths,
            StorageRegionManager storageRegionManager,
            StorageRegionWriter storageRegionWriter,
            ServiceManager serviceManager,
            Lifecycle lifecycle
            ) {
        String URI_DATA_ROOT = "/data";
        String URI_STREAM_DATA_ROOT = "streamData";
        String URI_STORAGE_REGION_ROOT = "/sr";
        
        int workerThreadNum = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_SERVER_IO_THREAD_NUM);
        String host = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_HOST);
        int port = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_PORT);
        
        HttpConfig httpConfig = HttpConfig.newBuilder()
                .setHost(host)
                .setPort(port)
                .setAcceptWorkerNum(1)
                .setRequestHandleWorkerNum(workerThreadNum)
                .setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048"))).build();
        NettyHttpServer httpServer = new NettyHttpServer(httpConfig);
        httpServer.addHttpAuthenticator(new HttpAuthenticator() {

            @Override
            public int check(String userName, String passwd) {
                StringBuilder tokenBuilder = new StringBuilder();
                tokenBuilder.append(userName).append(":").append(passwd);

                return simpleAuthentication.auth(tokenBuilder.toString()) ? 0 : ReturnCode.USER_FORBID.getCode();
            }

        });
        
        ExecutorService requestHandlerExecutor = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors() / 4),
                new PooledThreadFactory("request_handler"));
        
        NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
        requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(storageRegionWriter));
        requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
        requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler(paths, serviceManager, storageRegionManager));
        httpServer.addContextHandler(URI_DATA_ROOT, requestHandler);


        long blocksize = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_SIZE);
        int blockpool = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_CAPACITY);
        Integer initCount = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_BLOCK_POOL_INIT_COUNT);

        BlockPool blockPool = new BlockPool(blocksize, blockpool, initCount);
        BlockManager blockManager = new BlockManager(blockPool, storageRegionWriter);
        NettyHttpRequestHandler streamRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
        streamRequestHandler.addMessageHandler("Post",new WriteStreamDataMessageHandler(storageRegionWriter,blockManager));
        httpServer.addContextHandler(URI_STREAM_DATA_ROOT,streamRequestHandler);

        lifecycle.addLifeCycleObject(new LifeCycleObject() {
            
            @Override
            public void start() throws Exception {
                httpServer.start();
            }
            
            @Override
            public void stop() {
                requestHandlerExecutor.shutdown();
                httpServer.stop();
            }
            
        }, Lifecycle.Stage.SERVER);
        
        return httpServer;
    }
    
}
