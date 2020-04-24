package com.bonree.brfs.common.service.impl.curator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceCacheBuilder;

public class ServiceCacheBuilderImpl<T> implements ServiceCacheBuilder<T> {
    private ServiceDiscoveryImpl<T> discovery;
    private String name;
    private ThreadFactory threadFactory;
    private CloseableExecutorService executorService;

    ServiceCacheBuilderImpl(ServiceDiscoveryImpl<T> discovery) {
        this.discovery = discovery;
    }

    /**
     * Return a new service cache with the current settings
     *
     * @return service cache
     */
    @Override
    public ServiceCache<T> build() {
        if (executorService != null) {
            return new ServiceCacheImpl<T>(discovery, name, executorService);
        } else {
            return new ServiceCacheImpl<T>(discovery, name, threadFactory);
        }
    }

    /**
     * The name of the service to cache (required)
     *
     * @param name service name
     *
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Optional thread factory to use for the cache's internal thread
     *
     * @param threadFactory factory
     *
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        this.executorService = null;
        return this;
    }

    /**
     * Optional executor service to use for the cache's background thread
     *
     * @param executorService executor service
     *
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> executorService(ExecutorService executorService) {
        this.executorService = new CloseableExecutorService(executorService);
        this.threadFactory = null;
        return this;
    }

    /**
     * Optional CloseableExecutorService to use for the cache's background thread
     *
     * @param executorService an instance of CloseableExecutorService
     *
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> executorService(CloseableExecutorService executorService) {
        this.executorService = executorService;
        this.threadFactory = null;
        return this;
    }
}
