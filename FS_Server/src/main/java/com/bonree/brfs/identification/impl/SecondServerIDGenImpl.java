package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:49:32
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 使用zookeeper实现获取单副本服务标识，多副本服务标识，虚拟服务标识
 * 为了安全性，此处的方法，不需要太高的效率，故使用synchronized字段,该实例为单例模式
 ******************************************************************************/
public class SecondServerIDGenImpl implements LevelServerIDGen {
	private static final Logger LOG = LoggerFactory.getLogger(SecondServerIDGenImpl.class);
	
    private final static String SECOND_ID_INDEX_NODE = "secondIdIndex";
    public final static int SECOND_ID_PREFIX = 2;

    private SequenceNumberBuilder secondServerIdCreator;

    public SecondServerIDGenImpl(CuratorFramework client, String basePath) {
        this.secondServerIdCreator = new ZkSequenceNumberBuilder(client, ZKPaths.makePath(basePath, SECOND_ID_INDEX_NODE));
    }

    @Override
    public String genLevelID() {
		try {
			int uniqueId = secondServerIdCreator.nextSequenceNumber();
			
			StringBuilder idBuilder = new StringBuilder();
	    	idBuilder.append(SECOND_ID_PREFIX).append(uniqueId);
	    	
	    	return idBuilder.toString();
		} catch (Exception e) {
			LOG.error("create second id error", e);
		}
    	
		return null;
    }

}
