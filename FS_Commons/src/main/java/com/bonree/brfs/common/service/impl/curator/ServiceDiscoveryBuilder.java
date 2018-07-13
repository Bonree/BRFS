package com.bonree.brfs.common.service.impl.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

public class ServiceDiscoveryBuilder<T> {
    private CuratorFramework client;
    private String basePath;
    private InstanceSerializer<T> serializer;
    private ServiceInstance<T> thisInstance;
    private Class<T> payloadClass;
    private boolean watchInstances = false;

    /**
     * Return a new builder.
     *
     * @param payloadClass the class of the payload of your service instance (you can use {@link Void}
     *                     if your instances don't need a payload)
     * @return new builder
     */
    public static <T> ServiceDiscoveryBuilder<T> builder(Class<T> payloadClass)
    {
        return new ServiceDiscoveryBuilder<T>(payloadClass);
    }

    /**
     * Build a new service discovery with the currently set values. If not set, the builder will be
     * defaulted with a {@link JsonInstanceSerializer}.
     *
     * @return new service discovery
     */
    public ServiceDiscovery<T> build()
    {
        if ( serializer == null )
        {
            serializer(new JsonInstanceSerializer<T>(payloadClass));
        }
        return new ServiceDiscoveryImpl<T>(client, basePath, serializer, thisInstance, watchInstances);
    }

    /**
     * Required - set the client to use
     *
     * @param client client
     * @return this
     */
    public ServiceDiscoveryBuilder<T> client(CuratorFramework client)
    {
        this.client = client;
        return this;
    }

    /**
     * Required - set the base path to store in ZK
     *
     * @param basePath base path
     * @return this
     */
    public ServiceDiscoveryBuilder<T> basePath(String basePath)
    {
        this.basePath = basePath;
        return this;
    }

    /**
     * optional - change the serializer used (the default is {@link JsonInstanceSerializer}
     *
     * @param serializer the serializer
     * @return this
     */
    public ServiceDiscoveryBuilder<T> serializer(InstanceSerializer<T> serializer)
    {
        this.serializer = serializer;
        return this;
    }

    /**
     * Optional - instance that represents the service that is running. The instance will get auto-registered
     *
     * @param thisInstance initial instance
     * @return this
     */
    public ServiceDiscoveryBuilder<T> thisInstance(ServiceInstance<T> thisInstance)
    {
        this.thisInstance = thisInstance;
        return this;
    }

    /**
     * Optional - if true, watches for changes to locally registered instances
     * (via {@link #thisInstance(ServiceInstance)} or {@link ServiceDiscovery#registerService(ServiceInstance)}).
     * If the data for instances changes, they are reloaded.
     *
     * @param watchInstances true to watch instances
     * @return this
     */
    public ServiceDiscoveryBuilder<T> watchInstances(boolean watchInstances)
    {
        this.watchInstances = watchInstances;
        return this;
    }

    ServiceDiscoveryBuilder(Class<T> payloadClass)
    {
        this.payloadClass = payloadClass;
    }
}
