package com.bonree.brfs.duplication.datastream.file;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileLimiterFactory implements FileLimiterFactory {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileLimiterFactory.class); 
	
	private FileCoordinator coordinator;
	private DuplicationNodeSelector duplicationNodeSelector;
	private StorageNameManager storageNameManager;
	private Service service;
	private ServerIDManager idManager;
	private DiskNodeConnectionPool connectionPool;
	
	public DefaultFileLimiterFactory(FileCoordinator coordinator,
			DuplicationNodeSelector duplicationNodeSelector,
			StorageNameManager storageNameManager,
			Service service,
			ServerIDManager idManager,
			DiskNodeConnectionPool connectionPool) {
		this.coordinator = coordinator;
		this.duplicationNodeSelector = duplicationNodeSelector;
		this.storageNameManager = storageNameManager;
		this.service = service;
		this.idManager = idManager;
		this.connectionPool = connectionPool;
	}

	@Override
	public FileLimiter create(long time, int storageId) {
		StorageNameNode storageNameNode = storageNameManager.findStorageName(storageId);
		LOG.info("get storageNameNode-->{}, {}", storageId, storageNameNode);
		if(storageNameNode == null) {
			return null;
		}
		
		DuplicateNode[] nodes = duplicationNodeSelector.getDuplicationNodes(storageNameNode.getReplicateCount());
		
		FileNode fileNode = new FileNode(time);
		fileNode.setName(FileNameBuilder.createFile(idManager, storageId, nodes));
		fileNode.setStorageName(storageNameNode.getName());
		fileNode.setStorageId(storageNameNode.getId());
		fileNode.setServiceId(service.getServiceId());
		fileNode.setDuplicateNodes(nodes);
		
		FileLimiter fileLimiter = new FileLimiter(fileNode, DuplicationEnvironment.DEFAULT_MAX_FILE_SIZE);
		boolean headerWriting = false;
		for(DuplicateNode node : nodes) {
			DiskNodeConnection connection = connectionPool.getConnection(node);
			if(connection == null || connection.getClient() == null) {
				continue;
			}
			
			String serverId = idManager.getOtherSecondID(node.getId(), storageId);
			String filePath = FilePathBuilder.buildPath(fileNode, serverId);
			try {
				WriteResult result = connection.getClient().writeData(filePath, -1, fileLimiter.getHeader());
				if(result != null) {
					headerWriting = true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//如果没有一个磁盘节点写入头数据成功，则放弃使用此文件节点
		if(!headerWriting) {
			return null;
		}
		
		fileLimiter.setLength(fileLimiter.getHeader().length);
		try {
			coordinator.store(fileNode);
			//只有把文件信息成功存入文件库中才能使用此文件节点
			return fileLimiter;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
