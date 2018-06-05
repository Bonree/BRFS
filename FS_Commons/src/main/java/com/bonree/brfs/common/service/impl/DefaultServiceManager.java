package com.bonree.brfs.common.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.google.common.collect.HashMultimap;

/**
 * 服务管理接口的默认实现，借助Zookeeper的功能实现服务的管理流程
 * 和服务事件通知机制。
 * 
 * @author chen
 *
 */
public class DefaultServiceManager implements ServiceManager {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultServiceManager.class);
	
	//服务管理线程池名称
	private static final String DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME = "serviceManager";
	//默认的线程池大小
	private static final int DEFAULT_THREAD_POOL_SIZE = 5;
	private ExecutorService threadPools;
	
	//服务管理在ZK上的根节点
	private static final String SERVICE_BASE_PATH = "/discovery";
	private ServiceDiscovery<String> serviceDiscovery;
	private Map<String, ServiceCache<String>> serviceCaches = new HashMap<String, ServiceCache<String>>();
	private HashMultimap<String, ServiceStateListener> stateListeners;
	
	public DefaultServiceManager(CuratorFramework client) {
		this.threadPools = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE, new PooledThreadFactory(DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME));
		this.stateListeners = HashMultimap.create();
		this.serviceDiscovery = ServiceDiscoveryBuilder.builder(String.class)
				.client(client)
				.basePath(SERVICE_BASE_PATH)
				.build();
	}
	
	@Override
	public void start() throws Exception {
		serviceDiscovery.start();
	}
	
	@Override
	public void stop() {
		CloseableUtils.closeQuietly(serviceDiscovery);
		for(ServiceCache<String> cache : serviceCaches.values()) {
			CloseableUtils.closeQuietly(cache);
		}
	}
	
	private static ServiceInstance<String> buildFrom(Service service) throws Exception {
		return ServiceInstance.<String>builder()
				.address(service.getHost())
				.id(service.getServiceId())
				.name(service.getServiceGroup())
				.port(service.getPort())
				.registrationTimeUTC(service.getRegisterTime())
				.payload(service.getPayload())
				.build();
	}
	
	private static Service buildFrom(ServiceInstance<String> instance) {
		Service service = new Service();
		service.setServiceId(instance.getId());
		service.setServiceGroup(instance.getName());
		service.setHost(instance.getAddress());
		service.setPort(instance.getPort());
		service.setRegisterTime(instance.getRegistrationTimeUTC());
		service.setPayload(instance.getPayload());
		
		return service;
	}

	@Override
	public void registerService(Service service) throws Exception {
		LOG.info("registerService service[{}]", service);
		ServiceInstance<String> instance = buildFrom(service);
		
		serviceDiscovery.registerService(instance);
	}

	@Override
	public void unregisterService(Service service) throws Exception {
		LOG.info("unregisterService service[{}]", service);
		serviceDiscovery.unregisterService(serviceDiscovery.queryForInstance(service.getServiceGroup(), service.getServiceId()));
	}
	
	@Override
	public void updateService(String group, String serviceId, String payload) throws Exception {
		LOG.info("update service payload for service[{}, {}, {}]", group, serviceId, payload);
		Service service = getServiceById(group, serviceId);
		if(service == null) {
			throw new Exception("No service{group=" + group + ", id=" + serviceId + "} is found!");
		}
		
		service.setPayload(payload);
		serviceDiscovery.updateService(buildFrom(service));
	}
	
	private ServiceCache<String> getOrBuildServiceCache(String serviceGroup) {
		ServiceCache<String> serviceCache = serviceCaches.get(serviceGroup);
		if(serviceCache == null) {
			serviceCache = serviceDiscovery.serviceCacheBuilder().name(serviceGroup).executorService(threadPools).build();
			
			try {
				serviceCache.start();
				serviceCaches.put(serviceGroup, serviceCache);
				
				return serviceCache;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return serviceCache;
	}

	@Override
	public synchronized void addServiceStateListener(String serviceGroup, ServiceStateListener listener) throws Exception {
		LOG.info("add service state listener for group[{}]", serviceGroup);
		stateListeners.put(serviceGroup, listener);
		
		ServiceCache<String> serviceCache = getOrBuildServiceCache(serviceGroup);
		if(serviceCache == null) {
			throw new Exception("Can not get ServiceCache");
		}
		
		serviceCache.addListener(new InnerServiceListener(serviceGroup));
	}
	
	@Override
	public synchronized void removeServiceStateListenerByGroup(String group) {
		LOG.info("remove all service state listeners for group[{}]", group);
		stateListeners.removeAll(group);
	}
	
	@Override
	public synchronized void removeServiceStateListener(String serviceGroup, ServiceStateListener listener) {
		LOG.info("remove service state listener[{}] for group[{}]", listener, serviceGroup);
		stateListeners.remove(serviceGroup, listener);
	}
	
	private synchronized ServiceStateListener[] getServiceStateListener(String serviceGroup) {
		Set<ServiceStateListener> listenerSet = stateListeners.get(serviceGroup);
		ServiceStateListener[] listeners = new ServiceStateListener[listenerSet.size()];
		
		return listenerSet.toArray(listeners);
	}

	@Override
	public Service getServiceById(String group, String serviceId) {
		LOG.info("search service with group[{}], id[{}]", group, serviceId);
		for(Service service : getServiceListByGroup(group)) {
			if(service.getServiceId().equals(serviceId)) {
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
		
		if(serviceCache != null) {
			for(ServiceInstance<String> instance : serviceCache.getInstances()) {
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
	private class InnerServiceListener implements ServiceCacheListener {
		private final String group;
		private List<Service> lastCache;
		
		public InnerServiceListener(String group) {
			this.group = group;
			this.lastCache = getServiceListByGroup(group);
			
			for(Service service : lastCache) {
				notifyServiceAdded(service);
			}
		}
		
		private void notifyServiceAdded(Service service) {
			for(ServiceStateListener listener : getServiceStateListener(group)) {
				try {
					listener.serviceAdded(service);
				} catch (Exception e) {}
			}
		}
		
		private void notifyServiceRemoved(Service service) {
			for(ServiceStateListener listener : getServiceStateListener(group)) {
				try {
					listener.serviceRemoved(service);
				} catch (Exception e) {}
			}
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			//nothing to do!
			System.out.println("InnerServiceListener --" + newState.toString());
		}

		@Override
		public synchronized  void cacheChanged() {
			List<Service> tmp = getServiceListByGroup(group);
			
			for(Service last : lastCache) {
				if(!tmp.contains(last)) {
					notifyServiceRemoved(last);
				}
			}
			
			for(Service update : tmp) {
				if(!lastCache.contains(update)) {
					notifyServiceAdded(update);
				}
			}
			
			lastCache = tmp;
		}
		
	}
}
