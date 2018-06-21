package com.bonree.brfs.server.identification.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.sequencenumber.SequenceNumberBuilder;
import com.bonree.brfs.common.sequencenumber.ZkSequenceNumberBuilder;
import com.bonree.brfs.server.identification.LevelServerIDGen;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:49:32
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 使用zookeeper实现获取单副本服务标识，多副本服务标识，虚拟服务标识
 * 为了安全性，此处的方法，不需要太高的效率，故使用synchronized字段,该实例为单例模式
 ******************************************************************************/
public class FirstServerIDGenImpl implements LevelServerIDGen {
    private final static String FIRST_ID_INDEX_NODE = "firstIdIndex";
    public final static int FIRST_ID_PREFIX = 1;

    private SequenceNumberBuilder firstServerIDCreator;

    public FirstServerIDGenImpl(CuratorFramework client, String basePath) {
        this.firstServerIDCreator = new ZkSequenceNumberBuilder(client, ZKPaths.makePath(basePath, FIRST_ID_INDEX_NODE));
    }

    @Override
    public String genLevelID() {
    	int uniqueId = firstServerIDCreator.nextSequenceNumber();
    	if(uniqueId < 0) {
    		return null;
    	}
    	
    	StringBuilder idBuilder = new StringBuilder();
    	idBuilder.append(FIRST_ID_PREFIX).append(uniqueId);
    	
    	return idBuilder.toString();
    }

}
