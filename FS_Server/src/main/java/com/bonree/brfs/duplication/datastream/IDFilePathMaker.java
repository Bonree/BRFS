package com.bonree.brfs.duplication.datastream;

import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FilePathBuilder;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.server.identification.ServerIDManager;

public class IDFilePathMaker implements FilePathMaker {
	private ServerIDManager idManager;
	
	public IDFilePathMaker(ServerIDManager manager) {
		this.idManager = manager;
	}

	@Override
	public String buildPath(FileNode fileNode, DuplicateNode dupNode) {
		String serverId = idManager.getOtherSecondID(dupNode.getId(), fileNode.getStorageId());
		return FilePathBuilder.buildFilePath(fileNode, serverId);
	}

}
