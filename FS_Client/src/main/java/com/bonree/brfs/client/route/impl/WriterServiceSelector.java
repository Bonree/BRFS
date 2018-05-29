package com.bonree.brfs.client.route.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.ServiceSelector;
import com.bonree.brfs.common.service.Service;

public class WriterServiceSelector implements ServiceSelector {
    private ServiceMetaCache serviceMetaCache;
    private Random rand = new Random();

    public WriterServiceSelector(ServiceMetaCache serviceMetaCache) {
        this.serviceMetaCache = serviceMetaCache;
    }

    @Override
    public Service selectService() {
        Service service = null;
        List<String> firstIDs = new ArrayList<String>(serviceMetaCache.getServerCache().keySet());
        if (firstIDs != null && !firstIDs.isEmpty()) {

            String randomFirstID = firstIDs.get(rand.nextInt(firstIDs.size()));
            service = serviceMetaCache.getServerCache().get(randomFirstID);
        }
        return service;
    }

	@Override
	public List<Service> selectServiceList() {
		 List<String> firstIDs = new ArrayList<String>(serviceMetaCache.getServerCache().keySet());
		 List<Service> serviceList = new ArrayList<Service>(firstIDs.size());
		 if(firstIDs != null && !firstIDs.isEmpty()) {
			 int index = rand.nextInt(firstIDs.size());
			 for(int i = 0; i < firstIDs.size(); i++) {
				 String randomFirstID = firstIDs.get(index);
				 Service service = serviceMetaCache.getServerCache().get(randomFirstID);
				 if(service != null) {
					 serviceList.add(service);
				 }
				 
				 index = (index + 1) % firstIDs.size();
			 }
		 }
		 
		 return serviceList;
	}

}
