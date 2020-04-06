package com.bonree.brfs.identification;

import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 14:29:19
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 二级serverId信息维护信息基础类，
 ******************************************************************************/

public interface SecondMaintainerInterface extends SecondIdsInterface{
    /**
     * 注册二级serverid，当serverid已经注册过，则返回注册的serverid
     * @param partitionId
     * @param storageId
     * @return
     */
    String registerSecondId(String firstServer,String partitionId, int storageId);

    /**
     * 根据firstid和sotrageid 批量组测二级serverid，若一级serverid没有磁盘节点，则返回空
     * @param firstServer
     * @param storageId
     * @return
     */
    Collection<String> registerSecondIds(String firstServer,int storageId);

    /**
     * 注销二级serverid
     * @param partitionId
     * @param storageId
     * @return
     */
    boolean unregisterSecondId(String partitionId, int storageId);

    /**
     * 批量注销一级serverid为firstserver的二级serverid
     * @param firstServer
     * @param storageid
     * @return
     */
    boolean unregisterSecondIds(String firstServer,int storageid);

    /**
     * 判断二级serverid是否有效，有效为true，无效为false
     * @param secondId
     * @param storageId
     * @return
     */
    boolean isValidSecondId(String secondId, int storageId);

    /**
     * 添加 磁盘id与一级serverid关系
     * @param firstServer
     * @param partitionId
     */
    void addPartitionRelation(String firstServer,String partitionId);

    /**
     * 批量添加一级serverid与partitionId 的关系
     * @param parititionId
     * @param firstServer
     */
    void addAllPartitionRelation(Collection<String> parititionId, String firstServer);

    /**
     * 移除 磁盘id与一级serverid关系
     * @param partitionid
     * @return
     */
    boolean removePartitionRelation(String partitionid);

    /**
     * 批量移除磁盘id关系
     * @param partitionIds
     * @return
     */
    boolean removeAllPartitionRelation(Collection<String> partitionIds);

}
