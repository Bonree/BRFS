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
package com.bonree.brfs.client.discovery;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.bonree.brfs.client.ClientException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class CachedDiscovery implements Discovery {
    
    private static final Duration DEFAULT_EXPIRE_DURATION = Duration.ofMinutes(10);
    private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(5);
    
    private final Discovery delegate;
    
    private final LoadingCache<ServiceType, List<ServerNode>> nodeCache;
    private final ListeningExecutorService refreshExecutor;
    
    public CachedDiscovery(Discovery delegate, ExecutorService refreshExecutor, Duration expiredTime, Duration refreshTime) {
        this.delegate = delegate;
        this.refreshExecutor = MoreExecutors.listeningDecorator(refreshExecutor);
        this.nodeCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Optional.ofNullable(expiredTime).orElse(DEFAULT_EXPIRE_DURATION))
                .refreshAfterWrite(Optional.ofNullable(refreshTime).orElse(DEFAULT_REFRESH_DURATION))
                .build(new NodeLoader());
    }

    @Override
    public List<ServerNode> getServiceList(ServiceType type) {
        try {
            return nodeCache.get(type);
        } catch (ExecutionException e) {
            throw new ClientException(e, "can not load server nodes with type[%s]", type);
        }
    }
    
    private class NodeLoader extends CacheLoader<ServiceType, List<ServerNode>> {

        @Override
        public List<ServerNode> load(ServiceType type) throws Exception {
            return delegate.getServiceList(type);
        }

        @Override
        public ListenableFuture<List<ServerNode>> reload(ServiceType type, List<ServerNode> oldValue) throws Exception {
            return refreshExecutor.submit(() -> load(type));
        }
        
    }

    @Override
    public void close() throws IOException {
        refreshExecutor.shutdown();
    }
}
