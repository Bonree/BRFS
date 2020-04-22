package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月27日 15:22:31
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/
public class DiskNodeIDImpl implements LevelServerIDGen {
	private static final Logger LOG = LoggerFactory.getLogger(DiskNodeIDImpl.class);

    private final static String PARTITION_ID = "partitionIds";
    public final static int PARTITION_ID_PREFIX = 4;

    private SequenceNumberBuilder firstServerIDCreator;
    private CuratorFramework client = null;
    private String secondIdSetPath = null;
	@Inject
    public DiskNodeIDImpl(CuratorFramework client, String basePath,String secondIdSetPath) {
		this.client = client;
        this.firstServerIDCreator = new ZkSequenceNumberBuilder(this.client, ZKPaths.makePath(basePath, PARTITION_ID));
        this.secondIdSetPath = secondIdSetPath;

    }

    @Override
    public String genLevelID() {
		String partitionId = null;
		try {
			do {
				if(partitionId !=null){
					LOG.info("apple repeat partitionId {}",partitionId);
				}
				int uniqueId = firstServerIDCreator.nextSequenceNumber();

				StringBuilder idBuilder = new StringBuilder();
				idBuilder.append(PARTITION_ID_PREFIX).append(uniqueId);

				partitionId = idBuilder.toString();
			}while (this.client.checkExists().forPath(this.secondIdSetPath+"/"+partitionId) != null);
		} catch (Exception e) {
			LOG.info("create disk id error", e);
		}

		return partitionId;
    }

}
