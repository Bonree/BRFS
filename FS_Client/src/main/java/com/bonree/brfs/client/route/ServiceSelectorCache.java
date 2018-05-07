package com.bonree.brfs.client.route;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.impl.RandomServiceSelector;
import com.bonree.brfs.client.route.impl.ReaderServiceSelector;
import com.bonree.brfs.client.route.impl.WriterServiceSelector;
import com.bonree.brfs.common.service.Service;

public class ServiceSelectorCache {
    private ServiceMetaCache serviceMetaCache;
    private RouteParser routeParser;

    public ServiceSelectorCache(ServiceMetaCache serviceMetaCache, RouteParser routeParser) {
        this.serviceMetaCache = serviceMetaCache;
        this.routeParser = routeParser;
    }

    static ServiceSelector randomSelector = new RandomServiceSelector();
    static ServiceSelector writerSelector = new WriterServiceSelector();
    static ServiceSelector_1 readerSelector = new ReaderServiceSelector();

    public Service randomService() {
        return randomSelector.selectService(serviceMetaCache);
    }

    public Service writerService() {
        return writerSelector.selectService(serviceMetaCache);
    }

    public Service readerService(String partFid) {
        return readerSelector.selectService(serviceMetaCache, routeParser, partFid);
    }

}
