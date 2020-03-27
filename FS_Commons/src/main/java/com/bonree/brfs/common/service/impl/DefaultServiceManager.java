package com.bonree.brfs.common.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.service.impl.curator.ServiceDiscoveryBuilder;
import com.bonree.brfs.common.utils.PooledThreadFactory;

/**
 * 服务管理接口的默认实现，借助Zookeeper的功能实现服务的管理流程
 * 和服务事件通知机制。
 * 
 * @author yupeng
 *
 */
@ManageLifecycle
public class DefaultServiceManager implements ServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultServiceManager.class);

    // 服务管理线程池名称
    private static final String DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME = "serviceManager";
    // 默认的线程池大小
    private static final int DEFAULT_THREAD_POOL_SIZE = 5;
    private ExecutorService threadPools;

    // 服务管理在ZK上的根节点
    private static final String SERVICE_BASE_PATH = "/discovery";
    private ServiceDiscovery<String> serviceDiscovery;
    private Map<String, ServiceCache<String>> serviceCaches = new HashMap<String, ServiceCache<String>>();
    private Map<String, ServiceStateWatcher> watchers = new HashMap<String, ServiceStateWatcher>();

    @Inject
    public DefaultServiceManager(CuratorFramework client, ZookeeperPaths paths) {
        this.threadPools = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE,
                new PooledThreadFactory(DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME));
        this.serviceDiscovery = ServiceDiscoveryBuilder.builder(String.class)
                .client(client.usingNamespace(paths.getBaseClusterName().substring(1)))
                .basePath(SERVICE_BASE_PATH)
                .watchInstances(true)
                .build();
    }

    @LifecycleStart
    public void start() throws Exception {
        serviceDiscovery.start();
    }

    @LifecycleStop
    public void stop() {
        CloseableUtils.closeQuietly(serviceDiscovery);
        for (ServiceCache<String> cache : serviceCaches.values()) {
            CloseableUtils.closeQuietly(cache);
        }

        threadPools.shutdown();
    }

    private static ServiceInstance<String> buildFrom(Service service) throws Exception {
        return ServiceInstance.<String>builder().address(service.getHost()).id(service.getServiceId())
                .name(service.getServiceGroup()).port(service.getPort()).sslPort(service.getExtraPort())
                .registrationTimeUTC(service.getRegisterTime()).payload(service.getPayload()).build();
    }

    private static Service buildFrom(ServiceInstance<String> instance) {
        Service service = new Service();
        service.setServiceId(instance.getId());
        service.setServiceGroup(instance.getName());
        service.setHost(instance.getAddress());
        service.setPort(instance.getPort());
        service.setExtraPort(instance.getSslPort());
        service.setRegisterTime(instance.getRegistrationTimeUTC());
        service.setPayload(instance.getPayload());

        return service;
    }

    @Override
    public void registerService(Service service) throws Exception {
        LOG.info("registerService service[{}]", service);
        serviceDiscovery.registerService(buildFrom(service));
    }

    @Override
    public void unregisterService(Service service) throws Exception {
        LOG.info("unregisterService service[{}]", service);
        serviceDiscovery.unregisterService(buildFrom(service));
    }

    @Override
    public void updateService(String group, String serviceId, String payload) throws Exception {
        LOG.info("update service payload for service[{}, {}, {}]", group, serviceId, payload);
        Service service = getServiceById(group, serviceId);
        if (service == null) {
            throw new Exception("No service{group=" + group + ", id=" + serviceId + "} is found!");
        }

        service.setPayload(payload);
        serviceDiscovery.updateService(buildFrom(service));
    }

    private ServiceCache<String> getOrBuildServiceCache(String serviceGroup) {
        ServiceCache<String> serviceCache = serviceCaches.get(serviceGroup);
        if (serviceCache == null) {
            synchronized (serviceCaches) {
                serviceCache = serviceCaches.get(serviceGroup);
                if (serviceCache == null) {
                    serviceCache = serviceDiscovery.serviceCacheBuilder().name(serviceGroup)
                            .executorService(threadPools).build();

                    try {
                        serviceCache.start();
                        serviceCaches.put(serviceGroup, serviceCache);

                        LOG.info("build service cache for group[{}]", serviceGroup);
                        return serviceCache;
                    } catch (Exception e) {
                        LOG.error("build service cache error", e);
                    }
                }
            }
        }

        return serviceCache;
    }

    @Override
    public void addServiceStateListener(String serviceGroup, ServiceStateListener listener) throws Exception {
        LOG.info("add service state listener for group[{}]", serviceGroup);
        ServiceStateWatcher watcher = watchers.get(serviceGroup);
        if (watcher == null) {
            synchronized (watchers) {
                watcher = watchers.get(serviceGroup);
                if (watcher == null) {
                    ServiceCache<String> serviceCache = getOrBuildServiceCache(serviceGroup);
                    if (serviceCache == null) {
                        throw new Exception("Can not get ServiceCache");
                    }

                    watcher = new ServiceStateWatcher(serviceGroup);
                    serviceCache.addListener(watcher);
                    watchers.put(serviceGroup, watcher);
                }
            }
        }

        watcher.addListener(listener);
    }

    @Override
    public void removeServiceStateListenerByGroup(String serviceGroup) {
        LOG.info("remove all service state listeners for group[{}]", serviceGroup);
        ServiceStateWatcher watcher = watchers.get(serviceGroup);
        if (watcher == null) {
            return;
        }

        watcher.removeAllListeners();
    }

    @Override
    public void removeServiceStateListener(String serviceGroup, ServiceStateListener listener) {
        LOG.info("remove service state listener[{}] for group[{}]", listener, serviceGroup);
        ServiceStateWatcher watcher = watchers.get(serviceGroup);
        if (watcher == null) {
            return;
        }

        watcher.removeListener(listener);
    }

    @Override
    public Service getServiceById(String group, String serviceId) {
        LOG.info("search service with group[{}], id[{}]", group, serviceId);
        for (Service service : getServiceListByGroup(group)) {
            if (service.getServiceId().equals(serviceId)) {
                return service;
            }
        }

        return null;
    }

    @Override
    public List<Service> getServiceListByGroup(String serviceGroup) {
        LOG.info("search all services with group[{}]", serviceGroup);
        ArrayList<Service> serviceList = new ArrayList<Service>();
        ServiceCache<String> serviceCache = getOrBuildServiceCache(serviceGroup);

        if (serviceCache != null) {
            for (ServiceInstance<String> instance : serviceCache.getInstances()) {
                serviceList.add(buildFrom(instance));
            }
        }

        return serviceList;
    }

    /**
     * Curator服务状态变更事件的监听类
     * 
     * @author chen
     *
     */
    private class ServiceStateWatcher implements ServiceCacheListener {
        private final String group;
        private List<Service> lastCache;

        private List<ServiceStateListener> listeners = new ArrayList<ServiceStateListener>();

        public ServiceStateWatcher(String group) {
            this.group = group;
            this.lastCache = getServiceListByGroup(group);

            for (Service service : lastCache) {
                notifyServiceAdded(service);
            }
        }

        public synchronized void addListener(ServiceStateListener listener) {
            listeners.add(listener);

            for (Service service : lastCache) {
                try {
                    listener.serviceAdded(service);
                } catch (Exception e) {
                    LOG.error("notify service added error", e);
                }
            }
        }

        public synchronized void removeListener(ServiceStateListener listener) {
            listeners.remove(listener);
        }

        public synchronized void removeAllListeners() {
            listeners.clear();
        }

        private void notifyServiceAdded(Service service) {
            for (ServiceStateListener listener : listeners) {
                try {
                    listener.serviceAdded(service);
                } catch (Exception e) {
                    LOG.error("notify service added error", e);
                }
            }
        }

        private void notifyServiceRemoved(Service service) {
            for (ServiceStateListener listener : listeners) {
                try {
                    listener.serviceRemoved(service);
                } catch (Exception e) {
                    LOG.error("notify service removed error", e);
                }
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
        }

        @Override
        public synchronized void cacheChanged() {
            List<Service> tmp = getServiceListByGroup(group);

            for (Service last : lastCache) {
                if (!tmp.contains(last)) {
                    notifyServiceRemoved(last);
                }
            }

            for (Service update : tmp) {
                if (!lastCache.contains(update)) {
                    notifyServiceAdded(update);
                }
            }

            lastCache = tmp;
        }

    }
}
