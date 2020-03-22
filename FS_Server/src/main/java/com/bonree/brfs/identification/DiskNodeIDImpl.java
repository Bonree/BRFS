package com.bonree.brfs.identification;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年03月22日 12:41
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘id生成器,原理 根据zookeeper的自增序列生成
 **/
public class DiskNodeIDImpl implements LevelServerIDGen {
    private static final Logger LOG = LoggerFactory.getLogger(DiskNodeIDImpl.class);
    private static final String DISK_NODE_CHILD ="diskNodeID";
    public static final int DISK_INDEX_PRE=1;
    private SequenceNumberBuilder diskIdSequence;


    public DiskNodeIDImpl(CuratorFramework client, String basePath) {
        this.diskIdSequence = new ZkSequenceNumberBuilder(client, ZKPaths.makePath(basePath, DISK_NODE_CHILD));
    }


    @Override
    public String genLevelID() {
        try {
            int uniqueId = diskIdSequence.nextSequenceNumber();

            StringBuilder idBuilder = new StringBuilder();
            idBuilder.append(DISK_INDEX_PRE).append(uniqueId);
            return idBuilder.toString();
        } catch (Exception e) {
            LOG.info("create disk node id error", e);
        }

        return null;
    }
}
