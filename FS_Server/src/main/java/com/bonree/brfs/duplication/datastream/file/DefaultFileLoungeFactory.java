package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileLoungeFactory implements FileLoungeFactory {
	private FileLimiterFactory fileFactory;
	private StorageNameManager storageNameManager;
	
	public DefaultFileLoungeFactory(Service service,
			FileCoordinator fileCoordinator,
			DuplicationNodeSelector nodeSelector,
			StorageNameManager storageNameManager,
			ServerIDManager idManager,
			DiskNodeConnectionPool connectionPool) {
		this.storageNameManager = storageNameManager;
		this.fileFactory = new DefaultFileLimiterFactory(fileCoordinator, nodeSelector, storageNameManager, service, idManager, connectionPool);
	}

	@Override
	public FileLounge createFileLounge(int storageId) {
		StorageNameNode node = storageNameManager.findStorageName(storageId);
		if(node == null) {
			return null;
		}
		
		FileLounge fileLounge = new DefaultFileLounge(storageId, fileFactory);
		
		return fileLounge;
	}

	
}
