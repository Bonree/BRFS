package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileLoungeFactory implements FileLoungeFactory {
	private FileLimiterFactory fileFactory;
	
	public DefaultFileLoungeFactory(Service service,
			FileCoordinator fileCoordinator,
			DuplicationNodeSelector nodeSelector,
			StorageNameManager storageNameManager,
			ServerIDManager idManager) {
		this.fileFactory = new DefaultFileLimiterFactory(fileCoordinator, nodeSelector, storageNameManager, service, idManager);
	}

	@Override
	public FileLounge createFileLounge(int storageId) {
		return new DefaultFileLounge(storageId, fileFactory);
	}

}
