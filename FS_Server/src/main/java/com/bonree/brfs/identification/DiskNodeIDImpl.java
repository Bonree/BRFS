package com.bonree.brfs.identification;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/****************************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020-03-24 14:10:28
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 生成磁盘id序号服务
 ****************************************************************************************/
public class DiskNodeIDImpl implements LevelServerIDGen {
	private static final Logger LOG = LoggerFactory.getLogger(DiskNodeIDImpl.class);

    private final static String PARTITION_ID = "partitionIds";
    public final static int PARTITION_ID_PREFIX = 4;

    private SequenceNumberBuilder firstServerIDCreator;

    public DiskNodeIDImpl(CuratorFramework client, String basePath) {
        this.firstServerIDCreator = new ZkSequenceNumberBuilder(client, ZKPaths.makePath(basePath, PARTITION_ID));
    }

    @Override
    public String genLevelID() {
		try {
			int uniqueId = firstServerIDCreator.nextSequenceNumber();
			
			StringBuilder idBuilder = new StringBuilder();
	    	idBuilder.append(PARTITION_ID_PREFIX).append(uniqueId);
	    	
	    	return idBuilder.toString();
		} catch (Exception e) {
			LOG.info("create disk id error", e);
		}
		
		return null;
    }

}
