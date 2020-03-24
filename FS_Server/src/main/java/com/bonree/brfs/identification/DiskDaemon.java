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

/****************************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020-03-23 14:58:21
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区id生成器,根据配置文件的多个目录生成不同的serverId
 ****************************************************************************************/
public class DiskDaemon {
	private static final Logger LOG = LoggerFactory.getLogger(DiskDaemon.class);
	/**
	 * id自增序列
	 */
    private LevelServerIDGen diskNodeServerId;

	/**
	 * 服务内部文件，存储ben
	 */
	private String diskNodeIDFile;

    private CuratorFramework client;

    private String diskNodeZKPath;

    public DiskDaemon(CuratorFramework client, String diskNodeZKPath, String diskNodeIDFile, String seqZkPath,String dataDir) {
        this.client = client;
        this.diskNodeZKPath = diskNodeZKPath;
        this.diskNodeIDFile = diskNodeIDFile;
        diskNodeServerId = new DiskNodeIDImpl(client, seqZkPath);
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
			.forPath(ZKPaths.makePath(diskNodeZKPath, firstServerID),"HelloWorld".getBytes());
			
			Files.createParentDirs(idFile);
			Files.asCharSink(idFile, Charsets.UTF_8).write(firstServerID);
		} catch (Exception e) {
			LOG.error("can not persist server id[{}]", idFile.getAbsolutePath(), e);
			
			throw new RuntimeException("can not persist server id", e);
		}
		
		return firstServerID;
    }
}
