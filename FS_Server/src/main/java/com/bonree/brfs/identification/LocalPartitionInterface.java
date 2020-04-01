package com.bonree.brfs.identification;

import java.util.Collection;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月30日 15:46:44
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 本地磁盘id管理接口，主要负责本地磁盘分区转换为对应的路径
 ******************************************************************************/

public interface LocalPartitionInterface {
    /**
     * 根据磁盘id 获取磁盘分区的存储目录
     * @param partitionId
     * @return
     */
    String getDataPaths(String partitionId);

    /**
     * 根据文件的路径，获取磁盘id
     * @param dataPath
     * @return
     */
    String getPartitionId(String dataPath);

    /**
     * 列出本机所有的磁盘节点信息
     * @return
     */
    Collection<String> listPartitionId();
}
