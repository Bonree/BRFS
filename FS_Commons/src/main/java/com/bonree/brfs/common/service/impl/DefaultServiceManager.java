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
	//服务管理线程池名称
	private static final String DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME = "serviceManager";
	//默认的线程池大小
	private static final int DEFAULT_THREAD_POOL_SIZE = 5;
	private ExecutorService threadPools;
	
	//服务管理在ZK上的根节点
	private static final String SERVICE_BASE_PATH = "/discovery";
	private ServiceDiscovery<Service> serviceDiscovery;
	private Map<String, ServiceCache<Service>> serviceCaches = new HashMap<String, ServiceCache<Service>>();
	private HashMultimap<String, ServiceStateListener> stateListeners;
	
	public DefaultServiceManager(CuratorFramework client) {
		this.threadPools = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE, new PooledThreadFactory(DEFAULT_SERVICE_MANAGER_THREADPOOL_NAME));
		this.stateListeners = HashMultimap.create();
		this.serviceDiscovery = ServiceDiscoveryBuilder.builder(Service.class)
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
		for(ServiceCache<Service> cache : serviceCaches.values()) {
			CloseableUtils.closeQuietly(cache);
		}
	}
	
	private static ServiceInstance<Service> buildFrom(Service service) throws Exception {
		return ServiceInstance.<Service>builder()
				.address(service.getHost())
				.id(service.getServiceId())
				.name(service.getServiceGroup())
				.port(service.getPort())
				.build();
	}
	
	private static Service buildFrom(ServiceInstance<Service> instance) {
		Service service = new Service();
		service.setServiceId(instance.getId());
		service.setServiceGroup(instance.getName());
		service.setHost(instance.getAddress());
		service.setPort(instance.getPort());
		
		return service;
	}

	@Override
	public void registerService(Service service) throws Exception {
		ServiceInstance<Service> instance = buildFrom(service);
		
		serviceDiscovery.registerService(instance);
	}

	@Override
	public void unregisterService(Service service) throws Exception {
		serviceDiscovery.unregisterService(serviceDiscovery.queryForInstance(service.getServiceGroup(), service.getServiceId()));
	}

	@Override
	public synchronized void addServiceStateListener(String serviceGroup, ServiceStateListener listener) throws Exception {
		stateListeners.put(serviceGroup, listener);
		
		ServiceCache<Service> serviceCache = serviceCaches.get(serviceGroup);
		if(serviceCache == null) {
			serviceCache = serviceDiscovery.serviceCacheBuilder().name(serviceGroup).executorService(threadPools).build();
			serviceCaches.put(serviceGroup, serviceCache);
			
			serviceCache.start();
			serviceCache.addListener(new InnerServiceListener(serviceGroup));
		}
	}
	
	@Override
	public synchronized void removeServiceStateListenerByGroup(String group) {
		stateListeners.removeAll(group);
	}
	
	@Override
	public synchronized void removeServiceStateListener(String serviceGroup, ServiceStateListener listener) {
		stateListeners.remove(serviceGroup, listener);
	}
	
	private synchronized ServiceStateListener[] getServiceStateListener(String serviceGroup) {
		Set<ServiceStateListener> listenerSet = stateListeners.get(serviceGroup);
		ServiceStateListener[] listeners = new ServiceStateListener[listenerSet.size()];
		
		return listenerSet.toArray(listeners);
	}

	@Override
	public Service getServiceById(String group, String serviceId) {
		for(Service service : getServiceListByGroup(group)) {
			if(service.getServiceId().equals(serviceId)) {
				return service;
			}
		}
		
		return null;
	}
	
	@Override
	public List<Service> getServiceListByGroup(String serviceGroup) {
		ArrayList<Service> serviceList = new ArrayList<Service>();
		ServiceCache<Service> serviceCache = serviceCaches.get(serviceGroup);
		if(serviceCache != null) {
			for(ServiceInstance<Service> instance : serviceCache.getInstances()) {
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
		public void cacheChanged() {
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
