/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月20日 17:24:34
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

package com.bonree.brfs.common.rebalance.route.impl;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.impl.v1.NormalRouteV1;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "version")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = NormalRouteV1.class, name = "V1"),
    @JsonSubTypes.Type(value = NormalRouteV2.class, name = "V2")
})
public abstract class SuperNormalRoute implements NormalRouteInterface {
    /**
     * 变更ID
     */
    @JsonProperty("changeID")
    protected String changeID;
    /**
     * storageregion 的id
     */
    @JsonProperty("storageIndex")
    protected int storageIndex;
    /**
     * 二级serverid
     */
    @JsonProperty("secondID")
    protected String secondID;
    /**
     * 系统版本
     */
    @JsonProperty("version")
    protected TaskVersion version;

    @JsonCreator
    public SuperNormalRoute(@JsonProperty("changeID") String changeID,
                            @JsonProperty("storageIndex") int storageIndex,
                            @JsonProperty("secondID") String secondID,
                            @JsonProperty("version") TaskVersion version) {
        this.changeID = changeID;
        this.storageIndex = storageIndex;
        this.secondID = secondID;
        this.version = version;
    }

    /**
     * 文件名生成数值 V1版本使用的。V2兼容
     *
     * @param fileCode
     * @param size
     *
     * @return
     */
    protected int hashFileName(int fileCode, int size) {
        int matchSm = fileCode % size;
        return matchSm;
    }

    /**
     * 过滤
     *
     * @param services
     *
     * @return
     */
    protected List<String> filterService(Collection<String> newSecondIDs,
                                         Collection<String> services) {
        List<String> selectors = new ArrayList<>();
        // 1.过滤掉已经使用的service
        if (services != null && !services.isEmpty()) {
            for (String ele : newSecondIDs) {
                if (services.contains(ele)) {
                    continue;
                }
                selectors.add(ele);
            }
        } else {
            selectors.addAll(newSecondIDs);
        }
        // 2.判断集合是否为空，为空，则解析失败。
        // todo 1 定义无服务 的异常内容
        if (selectors.isEmpty()) {
            throw new IllegalArgumentException("route is invalid " + this.toString());
        }
        // 3.对select 服务进行排序。
        //Collections.sort(selectors, new Comparator<String>() {
        //    @Override
        //    public int compare(String o1, String o2) {
        //        return o1.compareTo(o2);
        //    }
        //});
        return selectors;
    }

    @JsonProperty("changeID")
    public String getChangeID() {
        return changeID;
    }

    @JsonProperty("changeID")
    public void setChangeID(String changeID) {
        this.changeID = changeID;
    }

    @JsonProperty("storageIndex")
    public int getStorageIndex() {
        return storageIndex;
    }

    @JsonProperty("storageIndex")
    public void setStorageIndex(int storageIndex) {
        this.storageIndex = storageIndex;
    }

    @JsonProperty("secondID")
    public String getSecondID() {
        return secondID;
    }

    @JsonProperty("secondID")
    public void setSecondID(String secondID) {
        this.secondID = secondID;
    }

    @JsonProperty("version")
    public TaskVersion getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(TaskVersion version) {
        this.version = version;
    }

    @Override
    @JsonIgnore
    public int getStorageRegionIndex() {
        return getStorageIndex();
    }

    @Override
    @JsonIgnore
    public String getBaseSecondId() {
        return getSecondID();
    }

    @Override
    @JsonIgnore
    public String getChangeId() {
        return getChangeID();
    }

    @Override
    @JsonIgnore
    public TaskVersion getRouteVersion() {
        return getVersion();
    }
}
