package com.bonree.brfs.client.route;

import com.bonree.brfs.client.meta.ServiceMetaCache;
import com.bonree.brfs.common.service.Service;

public interface ServiceSelector_1 {

    /** 概述：
     * @param serviceCache
     * @param routeParser
     * @param partFid UUID_s1_s2
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public Service selectService(ServiceMetaCache serviceCache, RouteParser routeParser, String partFid);
}
