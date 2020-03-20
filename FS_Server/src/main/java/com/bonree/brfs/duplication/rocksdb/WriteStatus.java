package com.bonree.brfs.duplication.rocksdb;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/20 11:00
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public enum WriteStatus {
    SUCCESS,
    FAILED,
    KEY_EXISTS
}
