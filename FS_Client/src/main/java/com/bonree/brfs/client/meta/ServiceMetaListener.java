package com.bonree.brfs.client.meta;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceStateListener;

public class ServiceMetaListener implements ServiceStateListener {
    private ServiceMetaCache serviceCache;

    public ServiceMetaListener(ServiceMetaCache serviceCache) {
        this.serviceCache = serviceCache;
    }

    @Override
    public void serviceAdded(Service service) {
        serviceCache.addService(service);
    }

    @Override
    public void serviceRemoved(Service service) {
        serviceCache.removeService(service);
    }

}
