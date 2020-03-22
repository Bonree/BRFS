package com.bonree.brfs.identification;

import com.bonree.brfs.server.identification.LevelServerIDGen;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月28日 下午3:19:46
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 1级serverID实例
 ******************************************************************************/
public class DiskNodeServerIDImpl {
	private static final Logger LOG = LoggerFactory.getLogger(DiskNodeServerIDImpl.class);

    private LevelServerIDGen diskNodeServerId;

    private String diskNodeIDFile;

    private CuratorFramework client;

    private String diskNodeZKPath;

    public DiskNodeServerIDImpl(CuratorFramework client, String diskNodeZKPath, String diskNodeIDFile, String seqPath) {
        this.client = client;
        this.diskNodeZKPath = diskNodeZKPath;
        this.diskNodeIDFile = diskNodeIDFile;
        diskNodeServerId = new DiskNodeIDImpl(client, seqPath);
        initOrLoadServerID();
    }

    /** 概述：加载一级ServerID
     * 一级ServerID是用于标识每个服务的，不同的服务的一级ServerID一定是不同的，
     * 所以不会出现线程安全的问题
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String initOrLoadServerID() {
    	String firstServerID = null;
    	
    	File idFile = new File(diskNodeIDFile);
    	if(idFile.exists()) {
    		try {
    			firstServerID = Files.asCharSource(idFile, Charsets.UTF_8).readFirstLine();
			} catch (IOException e) {
				LOG.error("read server id file[{}] error", idFile.getAbsolutePath(), e);
			}
    		
    		if(firstServerID == null) {
    			throw new RuntimeException("can not load server id from local file[" + idFile.getAbsolutePath() + "]");
    		}
    		
    		LOG.info("load server id from local file : {}", firstServerID);
    		return firstServerID;
    	}
    	
    	firstServerID = diskNodeServerId.genLevelID();
    	if(firstServerID == null) {
			throw new RuntimeException("can not get server id[" + idFile.getAbsolutePath() + "]");
		}
		
		try {
			client.create()
			.creatingParentContainersIfNeeded()
			.withMode(CreateMode.PERSISTENT)
			.forPath(ZKPaths.makePath(diskNodeZKPath, firstServerID));
			
			Files.createParentDirs(idFile);
			Files.asCharSink(idFile, Charsets.UTF_8).write(firstServerID);
		} catch (Exception e) {
			LOG.error("can not persist server id[{}]", idFile.getAbsolutePath(), e);
			
			throw new RuntimeException("can not persist server id", e);
		}
		
		return firstServerID;
    }
}
