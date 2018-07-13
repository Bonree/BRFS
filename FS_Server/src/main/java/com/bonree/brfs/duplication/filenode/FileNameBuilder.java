package com.bonree.brfs.duplication.filenode;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.server.identification.ServerIDManager;

public class FileNameBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FileNameBuilder.class);
	
	public static String createFile(ServerIDManager idManager, StorageNameNode storageRegion, DuplicateNode[] duplicateNodes) {
		StringBuilder builder = new StringBuilder();
		builder.append(UUID.randomUUID().toString().replaceAll("-", ""));
		
		for(DuplicateNode node : duplicateNodes) {
			builder.append('_').append(idManager.getOtherSecondID(node.getId(), storageRegion.getId()));
		}
		
		int virtualIdCount = storageRegion.getReplicateCount() - duplicateNodes.length;
		if(virtualIdCount > 0) {
			for(String virtualId : idManager.getVirtualServerID(storageRegion.getId(), virtualIdCount)) {
				LOG.info("get virtual id---{}", virtualId);
				builder.append('_').append(virtualId);
			}
		}
		
		return builder.toString();
	}
}
