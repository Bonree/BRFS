package com.bonree.brfs.client.route.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.ServiceSelector;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;

public class WriterServiceSelector implements ServiceSelector{
    
    private AvailableServerInterface loadSelector;

    @Override
    public Service selectService(ServiceMetaCache serviceCache) {
        // String perfectFirstID = null;
        // return serviceCache.getFirstServerCache(perfectFirstID);
        
//        loadSelector.selectAvailableServer(1)
        List<String> firstIDs = new ArrayList<String>(serviceCache.getFirstServerCache().keySet());
        Random random = new Random();
        String randomFirstID = firstIDs.get(random.nextInt(firstIDs.size()));
        return serviceCache.getFirstServerCache().get(randomFirstID);
    }

}
