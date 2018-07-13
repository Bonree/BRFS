package com.bonree.brfs.common.service.impl.curator;

import java.util.List;
import java.util.concurrent.ThreadFactory;

import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.ServiceProviderBuilder;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;

import com.google.common.collect.Lists;

public class ServiceProviderBuilderImpl<T> implements ServiceProviderBuilder<T> {
    private ServiceDiscoveryImpl<T> discovery;
    private String serviceName;
    private ProviderStrategy<T> providerStrategy;
    private ThreadFactory threadFactory;
    private List<InstanceFilter<T>> filters = Lists.newArrayList();
    private DownInstancePolicy downInstancePolicy = new DownInstancePolicy();

    public ServiceProvider<T> build()
    {
        return new ServiceProviderImpl<T>(discovery, serviceName, providerStrategy, threadFactory, filters, downInstancePolicy);
    }

    ServiceProviderBuilderImpl(ServiceDiscoveryImpl<T> discovery)
    {
        this.discovery = discovery;
    }

    /**
     * required - set the name of the service to be provided
     *
     * @param serviceName the name of the service
     * @return this
     */
    @Override
    public ServiceProviderBuilder<T> serviceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    /**
     * optional - set the provider strategy. The default is {@link RoundRobinStrategy}
     *
     * @param providerStrategy strategy to use
     * @return this
     */
    @Override
    public ServiceProviderBuilder<T> providerStrategy(ProviderStrategy<T> providerStrategy)
    {
        this.providerStrategy = providerStrategy;
        return this;
    }

    /**
     * optional - the thread factory to use for creating internal threads
     *
     * @param threadFactory factory to use
     * @return this
     */
    @Override
    public ServiceProviderBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
        this.threadFactory = threadFactory;
        return this;
    }

    @Override
    public ServiceProviderBuilder<T> downInstancePolicy(DownInstancePolicy downInstancePolicy)
    {
        this.downInstancePolicy = downInstancePolicy;
        return this;
    }

    @Override
    public ServiceProviderBuilder<T> additionalFilter(InstanceFilter<T> filter)
    {
        filters.add(filter);
        return this;
    }
}
