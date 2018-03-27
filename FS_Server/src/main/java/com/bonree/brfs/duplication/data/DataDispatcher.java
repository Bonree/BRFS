package com.bonree.brfs.duplication.data;

import java.util.Set;

import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.recovery.FileRecovery;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.google.common.collect.HashMultimap;

public class DataDispatcher {
	
	private HashMultimap<String, FileNode> storageNameFiles;
	
	private StorageNameManager storageNameManager;
	private FileCoordinator fileCoordinator;
	private DuplicationSelector duplicationSelector;
	private DataEmitter dataEmitter;
	private FileRecovery fileRecovery;
	
	public DataDispatcher(StorageNameManager storageNameManager,
			              FileCoordinator fileCoordinator,
			              DuplicationSelector selector,
			              DataEmitter dataEmitter,
			              FileRecovery fileRecovery) {
		this.storageNameManager = storageNameManager;
		this.fileCoordinator = fileCoordinator;
		this.duplicationSelector = selector;
		this.dataEmitter = dataEmitter;
		this.fileRecovery = fileRecovery;
	}
	
	public void addFileNode(FileNode fileNode) {
		StorageNameNode storageNameNode = storageNameManager.findStorageName(fileNode.getStorageName());
		
		storageNameFiles.put(storageNameNode.getName(), fileNode);
	}
	
	public void write(int storageId, byte[] datas, DataHandleCallback<DataWriteResult> callback) {
		StorageNameNode storageNameNode = storageNameManager.findStorageName(storageId);
		
		Set<FileNode> fileNodes = storageNameFiles.get(storageNameNode.getName());
		if(fileNodes.isEmpty()) {
			FileNode fileNode = new FileNode();
			fileNode.setName(FileNameBuilder.createFile());
			fileNode.setStorageName(storageNameNode.getName());
			fileNode.setServiceId("");//TODO get service id
			fileNode.setDuplicates(duplicationSelector.getDuplication(storageNameNode.getReplicates()));
			
			boolean published = fileCoordinator.publish(fileNode);
			if(published) {
				fileNodes.add(fileNode);
			}
		}
		
		FileNode fileNode = fileNodes.iterator().next();
		dataEmitter.emit(datas, fileNode, new EmitCallback(storageNameNode.getName(), fileNode, callback));
	}
	
	private class EmitCallback implements DataEmitter.WriteCallback {
		private String storageName;
		private FileNode fileNode;
		private DataHandleCallback<DataWriteResult> callback;
		
		public EmitCallback(String storageName, FileNode fileNode, DataHandleCallback<DataWriteResult> callback) {
			this.storageName = storageName;
			this.fileNode = fileNode;
			this.callback = callback;
		}

		@Override
		public void success(WriteResult result) {
			DataWriteResult writeResult = new DataWriteResult();
			//TODO 生成FID
			writeResult.setFid(null);
			callback.completed(writeResult);
		}

		@Override
		public void recover() {
			storageNameFiles.get(storageName).remove(fileNode);
			fileRecovery.recover(fileNode);
		}
		
	}
}
