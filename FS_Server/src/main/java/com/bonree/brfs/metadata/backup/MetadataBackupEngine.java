package com.bonree.brfs.metadata.backup;

import com.bonree.brfs.metadata.ZNode;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 14:28
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 元数据备份接口
 ******************************************************************************/
public interface MetadataBackupEngine {
    ZNode backup();
}
