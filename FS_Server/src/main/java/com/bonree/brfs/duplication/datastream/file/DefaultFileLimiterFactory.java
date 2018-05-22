package com.bonree.brfs.duplication.datastream.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
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
	
	public DefaultFileLimiterFactory(FileCoordinator coordinator,
			DuplicationNodeSelector duplicationNodeSelector,
			StorageNameManager storageNameManager,
			Service service,
			ServerIDManager idManager) {
		this.coordinator = coordinator;
		this.duplicationNodeSelector = duplicationNodeSelector;
		this.storageNameManager = storageNameManager;
		this.service = service;
		this.idManager = idManager;
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
		
		try {
			coordinator.store(fileNode);
			return new FileLimiter(fileNode, DuplicationEnvironment.DEFAULT_MAX_FILE_SIZE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
