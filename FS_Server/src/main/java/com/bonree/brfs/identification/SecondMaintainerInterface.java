package com.bonree.brfs.identification;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 14:29:19
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 二级serverId信息维护信息
 ******************************************************************************/

public interface SecondMaintainerInterface {
    /**
     * 注册二级serverid，当serverid已经注册过，则返回注册的serverid
     * @param partitionId
     * @param storageId
     * @return
     */
    String registerSecondId(String firstServer,String partitionId, int storageId);

    /**
     * 注销二级serverid
     * @param firstServer
     * @param partitionId
     * @param storageId
     * @return
     */
    boolean unregisterSecondId(String firstServer,String partitionId, int storageId);

    /**
     * 判断二级serverid是否有效，有效为true，无效为false
     * @param secondId
     * @param storageId
     * @return
     */
    boolean isValidSecondId(String secondId, int storageId);

    void addPartitionRelation(String firstServer,String partitionId);

    boolean removePartitionRelation(String partitionid);

}
