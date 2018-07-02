package com.bonree.brfs.client.route;

import java.util.List;

import com.bonree.brfs.client.meta.impl.DiskServiceMetaCache;
import com.bonree.brfs.client.route.impl.RandomServiceSelector;
import com.bonree.brfs.client.route.impl.ReaderServiceSelector;
import com.bonree.brfs.common.service.Service;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月8日 上午9:42:37
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 可供查询的sn服务缓存
 ******************************************************************************/
public class DiskServiceSelectorCache {

    private ReaderServiceSelector readServerSelector;
    private RandomServiceSelector randomServerSelecor;

    public DiskServiceSelectorCache(DiskServiceMetaCache diskServiceMetaCache, RouteParser routeParser) {
        readServerSelector = new ReaderServiceSelector(diskServiceMetaCache, routeParser);
        randomServerSelecor = new RandomServiceSelector(diskServiceMetaCache);
    }

    public Service randomService() {
        return randomServerSelecor.selectService();
    }

    public Service writerService() {
        return randomServerSelecor.selectService();
    }

    public ServiceMetaInfo readerService(String partFid, List<Integer> excludePot) {
        return readServerSelector.selectService(partFid, excludePot);
    }

}
