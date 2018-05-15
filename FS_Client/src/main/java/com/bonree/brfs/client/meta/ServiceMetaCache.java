package com.bonree.brfs.client.meta;

import java.util.Map;

import com.bonree.brfs.common.service.Service;

public interface ServiceMetaCache {

    public void addService(Service service);

    public void removeService(Service service);

    public Map<String, Service> getServerCache();
}
