package com.bonree.brfs.client.route;

import com.bonree.brfs.client.meta.ServiceMetaCache;

public interface ServiceSelector_1 {

    /** 概述：
     * @param serviceCache
     * @param routeParser
     * @param partFid UUID_s1_s2
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public ServiceMetaInfo selectService(ServiceMetaCache serviceCache, RouteParser routeParser, String partFid);
}
