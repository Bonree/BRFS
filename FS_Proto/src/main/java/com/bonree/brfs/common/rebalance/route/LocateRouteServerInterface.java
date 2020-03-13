/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月12日 14:57:46
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 路由规则解析接口
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route;

import java.util.Collection;

public interface LocateRouteServerInterface {
    /**
     * 通过路由规则定位
     * @param fileUUID 文件块的uuid
     * @param services
     * @return
     */
     String locateNormalServer(String fileUUID, Collection<String> services);
}
