/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月18日 17:38:29
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 适配路由规则，将V1版本的路由规则增加解析接口
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.v1;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.AbstractNormalRoute;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class NormalRouteV1 extends AbstractNormalRoute {
    private static final TaskVersion CURRENT_VERSION = TaskVersion.V1;
    @JsonProperty("changeID")
    private String changeID;

    @JsonProperty("storageIndex")
    private int storageIndex;

    @JsonProperty("secondID")
    private String secondID;

    @JsonProperty("newSecondIDs")
    private List<String> newSecondIDs;

    @JsonProperty("version")
    private TaskVersion version;

    @SuppressWarnings("unused")
    private NormalRouteV1() {
    }

    public NormalRouteV1(String changeID, int storageIndex, String secondID, List<String> newSecondIDs) {
        this.changeID = changeID;
        this.storageIndex = storageIndex;
        this.secondID = secondID;
        this.newSecondIDs = newSecondIDs;
        this.version = CURRENT_VERSION;
    }

    @Override
    public String locateNormalServer(String fileUUID, Collection<String> services) {
        // 去掉不需要的服务
        List<String> selectors = filterService(this.newSecondIDs,services);
        // 在可用服务中获取id
        int index = hashFileName(fileUUID,selectors.size());
        // 返回选区的结果
        return selectors.get(index);
    }
}
