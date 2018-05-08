package com.bonree.brfs.client.route;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.client.route.impl.RandomServiceSelector;
import com.bonree.brfs.client.route.impl.ReaderServiceSelector;
import com.bonree.brfs.client.route.impl.WriterServiceSelector;
import com.bonree.brfs.common.service.Service;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月8日 上午9:42:37
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 可供查询的sn服务缓存
 ******************************************************************************/
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
