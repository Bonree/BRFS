package com.bonree.brfs.identification;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.partition.PartitionInfoRegister;
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
	 * 磁盘节点更新work
	 */
	private PartitionInfoRegister worker;
	/**
	 * 本地
	 */
	private Service localService;

	/**
	 * 服务内部文件，存储本地磁盘信息，此配置文件为基准
	 */
	private String diskNodeIDFile;

    private CuratorFramework client;

    private String diskNodeZKPath;

    public DiskDaemon(CuratorFramework client, String diskNodeZKPath, String diskNodeIDFile, String seqZkPath,String dataDir) {
        this.client = client;
        this.diskNodeZKPath = diskNodeZKPath;
        this.diskNodeIDFile = diskNodeIDFile;
        diskNodeServerId = new DiskNodeIDImpl(client, seqZkPath);

    }

    /** 概述：加载一级ServerID
     * 一级ServerID是用于标识每个服务的，不同的服务的一级ServerID一定是不同的，
     * 所以不会出现线程安全的问题
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String initDiskNodeID(String diskNodePath) {
    	String diskNodeId = null;
    	
    	File idFile = new File(diskNodeIDFile);
    	if(idFile.exists()) {
    		try {
    			diskNodeId = Files.asCharSource(idFile, Charsets.UTF_8).readFirstLine();
			} catch (IOException e) {
				LOG.error("read server id file[{}] error", idFile.getAbsolutePath(), e);
			}
    		
    		if(diskNodeId == null) {
    			throw new RuntimeException("can not load server id from local file[" + idFile.getAbsolutePath() + "]");
    		}
    		
    		LOG.info("load server id from local file : {}", diskNodeId);
    		return diskNodeId;
    	}
    	
    	diskNodeId = diskNodeServerId.genLevelID();
    	if(diskNodeId == null) {
			throw new RuntimeException("can not get server id[" + idFile.getAbsolutePath() + "]");
		}
		
		try {
			client.create()
			.creatingParentContainersIfNeeded()
			.withMode(CreateMode.PERSISTENT)
			.forPath(ZKPaths.makePath(diskNodeZKPath, diskNodeId),"HelloWorld".getBytes());
			
			Files.createParentDirs(idFile);
			Files.asCharSink(idFile, Charsets.UTF_8).write(diskNodeId);
		} catch (Exception e) {
			LOG.error("can not persist server id[{}]", idFile.getAbsolutePath(), e);
			
			throw new RuntimeException("can not persist server id", e);
		}
		
		return diskNodeId;
    }
}
