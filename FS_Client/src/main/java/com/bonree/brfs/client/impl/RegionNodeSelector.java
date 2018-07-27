package com.bonree.brfs.client.impl;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;

public class RegionNodeSelector {
	private ServiceManager serviceManager;
	private List<Service> regionServices = new CopyOnWriteArrayList<Service>();
	private Random random = new Random();
	
	public RegionNodeSelector(ServiceManager serviceManager, String serviceGroup) throws Exception {
		this.serviceManager = serviceManager;
		this.serviceManager.addServiceStateListener(serviceGroup, new RegionServiceListener());
	}
	
	public int serviceNum() {
		return regionServices.size();
	}
	
	public Service select() {
		return regionServices.get(random.nextInt(regionServices.size()));
	}
	
	public Service[] select(int n) {
		Service[] services = new Service[regionServices.size()];
		regionServices.toArray(services);
		
		Service[] results = new Service[Math.min(n, services.length)];
		for(int i = 0; i < results.length; i++) {
			int index = random.nextInt(services.length);
			while(true) {
				if(services[index] != null) {
					results[i] = services[index];
					services[index] = null;
					break;
				}
				
				index = (index + 1) % services.length;
			}
		}
		
		return results;
	}
	
	private class RegionServiceListener implements ServiceStateListener {

		@Override
		public void serviceAdded(Service service) {
			if(!regionServices.contains(service)) {
				regionServices.add(service);
			}
		}

		@Override
		public void serviceRemoved(Service service) {
			regionServices.remove(service);
		}
		
	}
}
