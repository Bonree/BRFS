package com.bonree.brfs.rebalance.route;

import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月31日 11:23:01
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public interface RouteLoader {
    /**
     * 加载虚拟serverId 路由规则
     *
     * @param storageRegionId
     *
     * @return
     *
     * @throws Exception
     */
    Collection<VirtualRoute> loadVirtualRoutes(int storageRegionId) throws Exception;

    /**
     * 加载正常路由规则
     *
     * @param storageRegionId
     *
     * @return
     *
     * @throws Exception
     */
    Collection<NormalRouteInterface> loadNormalRoutes(int storageRegionId) throws Exception;
}
