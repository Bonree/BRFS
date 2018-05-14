package com.bonree.brfs.duplication.coordinator;

import java.util.UUID;

import com.bonree.brfs.server.identification.ServerIDManager;

public class FileNameBuilder {
	
	public static String createFile(ServerIDManager idManager, int storageNameId, DuplicateNode[] duplicateNodes) {
		StringBuilder builder = new StringBuilder();
		builder.append(UUID.randomUUID().toString().replaceAll("-", ""));
		
		for(DuplicateNode node : duplicateNodes) {
			System.out.println("build name########" + node.getId() + ", sn " + storageNameId);
			builder.append('_').append(idManager.getOtherSecondID(node.getId(), storageNameId));
		}
		
		return builder.toString();
	}
}
