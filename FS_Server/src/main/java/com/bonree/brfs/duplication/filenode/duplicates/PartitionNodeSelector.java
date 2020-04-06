package com.bonree.brfs.duplication.filenode.duplicates;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 23:25
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public interface PartitionNodeSelector {
    /**
     * 根据一级serverid获取磁盘id
     * @param firstId
     * @return
     */
    String getPartitionId(String firstId);
}
