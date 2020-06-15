package com.bonree.brfs.rebalance.route;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.Pair;
import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 17:21:45
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 文件块解析接口，将文件块名称解析为对应的逻辑
 ******************************************************************************/

public interface BlockAnalyzer {
    /**
     * 判断serverid是否为虚拟serverid
     *
     * @param serverID
     *
     * @return
     */
    static boolean isVirtualID(String serverID) {
        return serverID.charAt(0) == Constants.VIRTUAL_ID;
    }

    /**
     * 解析文件块名称
     *
     * @param fileBocker 文件块名称
     *
     * @return Pair\<String,List\<String>> key: 文件的uuid，value 二级serverId集合，按照其顺序排列
     */
    static Pair<String, List<String>> analyzingFileName(String fileBocker) {
        String[] splitStr = fileBocker.split("_");
        String fileUUID = splitStr[0];
        List<String> fileServerIDs = new ArrayList<>(splitStr.length - 1);
        for (int i = 1; i < splitStr.length; i++) {
            fileServerIDs.add(splitStr[i]);
        }
        return new Pair<>(fileUUID, fileServerIDs);
    }

    /**
     * 根据文件名生成code
     *
     * @param name
     *
     * @return
     */
    static int sumName(String name) {
        int sum = 0;
        for (int i = 0; i < name.length(); i++) {
            sum = sum + name.charAt(i);
        }
        return sum;
    }

    /**
     * 根据文件块解析可用的服务
     *
     * @param fileName
     *
     * @return
     */
    String[] searchVaildIds(String fileName);

    /**
     * 更新所有路由规则
     */
    void update();

    /**
     * 添加虚拟路由规则
     *
     * @param virtualRoute
     */
    void putVirtualRoute(VirtualRoute virtualRoute);

    /**
     * 添加正常路由规则
     *
     * @param routeInterface
     */
    void putNormalRoute(NormalRouteInterface routeInterface);

    /**
     * 是否发布路由规则
     * @param secondId
     * @return
     */
    boolean isRoute(String secondId);
}
