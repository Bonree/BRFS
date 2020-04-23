/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月12日 14:57:46
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 路由规则解析接口
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route;

import com.bonree.brfs.common.rebalance.TaskVersion;
import java.util.Collection;
import java.util.Map;

public interface NormalRouteInterface {
    /**
     * 获取 storageRegion id
     *
     * @return
     */
    int getStorageRegionIndex();

    /**
     * 获取 路由规则的serverId
     *
     * @return
     */
    String getBaseSecondId();

    /**
     * 获取变更Id
     *
     * @return
     */
    String getChangeId();

    /**
     * 获取版本信息
     *
     * @return
     */
    TaskVersion getRouteVersion();

    /**
     * 通过路由规则定位
     *
     * @param fileUUIDCode 文件块的uuidcode
     * @param services
     *
     * @return
     */
    String locateNormalServer(int fileUUIDCode, Collection<String> services);

    /**
     * 获取路由规则内容，该方法只获取V2版本的路由规则
     *
     * @return
     */
    Map<String, Integer> getRoutes();
}
