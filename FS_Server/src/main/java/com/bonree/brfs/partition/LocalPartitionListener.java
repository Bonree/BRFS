package com.bonree.brfs.partition;

import com.bonree.brfs.partition.model.LocalPartitionInfo;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月25日 20:14:21
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘信息更新
 ******************************************************************************/

public interface LocalPartitionListener {
    void remove(LocalPartitionInfo partitionInfo);
    void add(LocalPartitionInfo partitionInfo);
}
