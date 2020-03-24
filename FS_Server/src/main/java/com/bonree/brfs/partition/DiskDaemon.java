package com.bonree.brfs.partition;

import com.bonree.brfs.common.process.LifeCycle;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月23日 16:47:58
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区守护线程，主要负责本地磁盘节点信息维护，负责zookeeper磁盘节点的注册，注销，更新
 ******************************************************************************/

public class DiskDaemon implements LifeCycle {
    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    public String getPartitionPath(int storageRegionId,String secondId){
        return null;
    }
    public String  getPartitionPath(String diskNodeId){
        return null;
    }
}
