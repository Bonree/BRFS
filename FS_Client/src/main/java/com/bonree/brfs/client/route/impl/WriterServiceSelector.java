package com.bonree.brfs.client.route.impl;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.ServiceSelector;
import com.bonree.brfs.common.service.Service;

public class WriterServiceSelector implements ServiceSelector {

    @Override
    public Service selectService(ServiceMetaCache serviceCache) {
        String perfectFirstID = null;
        return serviceCache.getFirstServerCache(perfectFirstID);
    }

}
