package com.bonree.brfs.client.route.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.ServiceSelector;
import com.bonree.brfs.common.service.Service;

public class WriterServiceSelector implements ServiceSelector {
    private ServiceMetaCache serviceMetaCache;

    public WriterServiceSelector(ServiceMetaCache serviceMetaCache) {
        this.serviceMetaCache = serviceMetaCache;
    }

    @Override
    public Service selectService() {
        Service service = null;
        List<String> firstIDs = new ArrayList<String>(serviceMetaCache.getServerCache().keySet());
        if (firstIDs != null && !firstIDs.isEmpty()) {

            Random random = new Random();
            String randomFirstID = firstIDs.get(random.nextInt(firstIDs.size()));
            service = serviceMetaCache.getServerCache().get(randomFirstID);
        }
        return service;
    }

}
