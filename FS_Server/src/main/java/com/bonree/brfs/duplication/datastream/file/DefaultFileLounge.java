package com.bonree.brfs.duplication.datastream.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.bonree.brfs.duplication.DuplicationNodeSelector;
import com.bonree.brfs.duplication.ServiceIdBuilder;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.google.common.collect.HashMultimap;

public class DefaultFileLounge implements FileLounge {
	private HashMultimap<String, FileLimiter> storageNameFiles;

	private StorageNameManager storageNameManager;
	private FileCoordinator fileCoordinator;
	private DuplicationNodeSelector duplicationSelector;
	
	public DefaultFileLounge(StorageNameManager storageNameManager, FileCoordinator fileCoordinator, DuplicationNodeSelector selector) {
		this.storageNameManager = storageNameManager;
		this.fileCoordinator = fileCoordinator;
		this.duplicationSelector = selector;
		this.storageNameFiles = HashMultimap.create();
	}

	@Override
	public FileLimiter getFileInfo(int storageNameId, int size) throws Exception {
		if(size > FileLimiter.DEFAULT_FILE_CAPACITY) {
			throw new DataSizeOverFlowException(size, FileLimiter.DEFAULT_FILE_CAPACITY);
		}
		
		StorageNameNode storageNameNode = storageNameManager.findStorageName(storageNameId);
		if(storageNameNode == null) {
			throw new StorageNameNonexistentException(storageNameId);
		}
		
		Set<FileLimiter> fileLimiters = storageNameFiles.get(storageNameNode.getName());
		
		for(FileLimiter limiter : fileLimiters) {
			if(limiter.obtain(size)) {
				return limiter;
			}
		}
		
		FileNode fileNode = new FileNode();
		fileNode.setName(FileNameBuilder.createFile());
		fileNode.setStorageName(storageNameNode.getName());
		fileNode.setServiceId(ServiceIdBuilder.getServiceId());//TODO get service id
		fileNode.setDuplicateNodes(duplicationSelector.getDuplicationNodes(storageNameNode.getReplicateCount()));
		fileCoordinator.store(fileNode);
		
		FileLimiter fileLimiter = new FileLimiter();
		fileLimiter.setFileNode(fileNode);
		fileLimiter.obtain(size);
		fileLimiters.add(fileLimiter);
		
		return fileLimiter;
	}
	
	@Override
	public List<FileLimiter> getFileLimiterList() {
		ArrayList<FileLimiter> fileList = new ArrayList<FileLimiter>();
		fileList.addAll(storageNameFiles.values());
		return fileList;
	}

	@Override
	public void deleteFile(FileLimiter file) {
		storageNameFiles.get(file.getFileNode().getStorageName()).remove(file);
		try {
			fileCoordinator.delete(file.getFileNode());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
