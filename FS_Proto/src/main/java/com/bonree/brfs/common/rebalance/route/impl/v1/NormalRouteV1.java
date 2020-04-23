/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月18日 17:38:29
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 适配路由规则，将V1版本的路由规则增加解析接口
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.impl.v1;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NormalRouteV1 extends SuperNormalRoute {
    private static final TaskVersion CURRENT_VERSION = TaskVersion.V1;
    private List<String> newSecondIDs;
    @JsonIgnore
    private Map<String, Integer> routes;

    public NormalRouteV1(@JsonProperty("changeID") String changeID,
                         @JsonProperty("storageIndex") int storageIndex,
                         @JsonProperty("secondID") String secondID,
                         @JsonProperty("newSecondIDs") List<String> newSecondIDs) {
        super(changeID, storageIndex, secondID, CURRENT_VERSION);
        this.newSecondIDs = newSecondIDs;
        routes = new HashMap<>();
        for (String id : this.newSecondIDs) {
            routes.put(id, 1);
        }
    }

    @Override
    public String locateNormalServer(int fileUUIDCode, Collection<String> services) {
        // 去掉不需要的服务
        List<String> selectors = filterService(this.newSecondIDs, services);
        // 在可用服务中获取id
        int index = hashFileName(fileUUIDCode, selectors.size());
        // 返回选区的结果
        return selectors.get(index);
    }

    @Override
    public Map<String, Integer> getRoutes() {
        return routes;
    }

    public List<String> getNewSecondIDs() {
        return newSecondIDs;
    }

    public void setNewSecondIDs(List<String> newSecondIDs) {
        this.newSecondIDs = newSecondIDs;
    }

    @Override
    public String toString() {
        return "NormalRouteV1{"
            +
            "newSecondIDs=" + newSecondIDs
            +
            ", changeID='" + changeID + '\''
            +
            ", storageIndex=" + storageIndex
            +
            ", secondID='" + secondID + '\''
            +
            ", version=" + version
            +
            '}';
    }
}
