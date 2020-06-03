package com.bonree.brfs.resource.command.identification;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月27日 15:22:31
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
public class DiskNodeIDImpl {

    private static final String PARTITION_ID = "partitionIds";
    public static final int PARTITION_ID_PREFIX = 4;

    private SequenceNumberBuilder firstServerIDCreator;
    private CuratorFramework client = null;
    private String secondIdSetPath = null;

    @Inject
    public DiskNodeIDImpl(CuratorFramework client, String basePath, String secondIdSetPath) {
        this.client = client;
        this.firstServerIDCreator = new ZkSequenceNumberBuilder(this.client, ZKPaths.makePath(basePath, PARTITION_ID));
        this.secondIdSetPath = secondIdSetPath;

    }

    public String genLevelID() throws Exception {
        String partitionId = null;
        try {
            do {
                int uniqueId = firstServerIDCreator.nextSequenceNumber();
                StringBuilder idBuilder = new StringBuilder();
                idBuilder.append(PARTITION_ID_PREFIX).append(uniqueId);
                partitionId = idBuilder.toString();
            } while (this.client.checkExists().forPath(this.secondIdSetPath + "/" + partitionId) != null);
        } catch (Exception e) {
            throw new RuntimeException("get partition id happen error", e);
        }

        return partitionId;
    }

}
