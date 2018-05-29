package com.bonree.brfs.client.route;

import java.util.List;

import com.bonree.brfs.client.meta.impl.DuplicaServiceMetaCache;
import com.bonree.brfs.client.route.impl.RandomServiceSelector;
import com.bonree.brfs.common.service.Service;

public class DuplicaServiceSelector {
    private RandomServiceSelector randomServiceSelector;

    public DuplicaServiceSelector(DuplicaServiceMetaCache duplicaServiceMetaCache) {
        this.randomServiceSelector = new RandomServiceSelector(duplicaServiceMetaCache);
    }

    public Service randomService() {
        return randomServiceSelector.selectService();
    }
    
    public List<Service> randomServiceList() {
    	return randomServiceSelector.selectServiceList();
    }
}
