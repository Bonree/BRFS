package com.bonree.brfs.client.route;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.common.service.Service;

public interface ServiceSelector_1 {

    /** 概述：选择一个Service
     * @param params
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectService(ServiceMetaCache serviceCache, RouteParser routeParser, String partFid);
}
