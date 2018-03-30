package com.bonree.brfs.duplication.datastream.file;

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
	private HashMultimap<String, FileInfo> storageNameFiles;

	private StorageNameManager storageNameManager;
	private FileCoordinator fileCoordinator;
	private DuplicationNodeSelector duplicationSelector;
	
	public DefaultFileLounge() {
		this.storageNameFiles = HashMultimap.create();
	}

	@Override
	public FileInfo getFileInfo(int storageNameId, int size) throws Exception {
		if(size > FileInfo.DEFAULT_FILE_CAPACITY) {
			throw new DataSizeOverFlowException(size, FileInfo.DEFAULT_FILE_CAPACITY);
		}
		
		StorageNameNode storageNameNode = storageNameManager.findStorageName(storageNameId);
		if(storageNameNode == null) {
			throw new StorageNameNonexistentException(storageNameId);
		}
		
		Set<FileInfo> fileInfos = storageNameFiles.get(storageNameNode.getName());
		
		for(FileInfo info : fileInfos) {
			if(info.remaining() >= size) {
				return info;
			}
		}
		
		FileNode fileNode = new FileNode();
		fileNode.setName(FileNameBuilder.createFile());
		fileNode.setStorageName(storageNameNode.getName());
		fileNode.setServiceId(ServiceIdBuilder.getServiceId());//TODO get service id
		fileNode.setDuplicateNodes(duplicationSelector.getDuplicationNodes(storageNameNode.getReplicateCount()));
		
		fileCoordinator.store(fileNode);
		FileInfo info = new FileInfo();
		info.setFileNode(fileNode);
		fileInfos.add(info);
		
		return info;
	}

	@Override
	public void deleteFile(FileInfo file) {
		storageNameFiles.get(file.getFileNode().getStorageName()).remove(file);
	}

}
