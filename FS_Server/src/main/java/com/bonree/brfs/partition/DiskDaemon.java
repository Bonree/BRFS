package com.bonree.brfs.partition;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.identification.DiskNodeIDImpl;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月23日 16:47:58
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区守护线程，主要负责本地磁盘节点信息维护，负责zookeeper磁盘节点的注册，注销，更新
 ******************************************************************************/

public class DiskDaemon implements LifeCycle {
    /**
     * 磁盘id生成器
     */
    private LevelServerIDGen idGen;
    /**
     * 路径对应id号
     */
    private Map<String,String> idPathMap;
    /**
     * 配置文件的目录
     */
    private List<String> dataDirs;
    /**
     * 有效的分区map
     */
    private Map<String,LocalPartitionInfo> vaildPartitionMap;

    /**
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }


}
