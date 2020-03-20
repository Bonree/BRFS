/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月19日 11:21:43
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 根据传入的byte[] 数组生成路由规则
 ******************************************************************************/

package com.bonree.brfs.rebalance.route.factory;

import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.impl.SuperNormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.JsonUtils;

public class SingleRouteFactory {
    private final static String VERSION_FIELD = "version";
    /**
     * 生成正常的路由规则
     * @param data
     * @return
     */
    public static NormalRouteInterface createRoute(byte[] data){
        //1. 输入参数检查，若参数为空则抛出异常
        if(data == null || data.length ==0){
            throw new IllegalArgumentException("Invalid input !! It's null or empty !!");
        }
        // 反序列化为map，若map为空则抛出异常，若map不包含版本信息则抛出版本异常信息
        NormalRouteInterface normal = null;
        try {
            normal = JsonUtils.toObject(data, SuperNormalRoute.class);
        } catch (JsonUtils.JsonException e) {
            throw new IllegalArgumentException("Invalid input !! content: "+new String(data));
        }
        if(normal == null){
            throw new IllegalArgumentException("Invalid input !! It's empty !!");
        }
        TaskVersion checkVersion = normal.getRouteVersion();
        if(checkVersion == null){
            throw new IllegalArgumentException("No version information!! It's empty !!");
        }
        if(TaskVersion.V1.equals(checkVersion) || TaskVersion.V2.equals(checkVersion)){
            return normal;
        }
        throw new IllegalArgumentException("Cannot support versions higher than 2.0! content:"+new String(data));
    }

    /**
     * 虚拟serverId 反序列化
     * @param data
     * @return
     * @throws JsonUtils.JsonException
     */
    public static VirtualRoute createVirtualRoute(byte[] data) {
        //1. 输入参数检查，若参数为空则抛出异常
        if(data == null || data.length ==0){
            throw new IllegalArgumentException("Invalid input !! It's null or empty !!");
        }
        // 2.反序列化
        return JsonUtils.toObjectQuietly(data,VirtualRoute.class);
    }
}
