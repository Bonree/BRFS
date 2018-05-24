package com.bonree.brfs.duplication.coordinator;

import java.util.UUID;

import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.server.identification.ServerIDManager;

public class FileNameBuilder {
	
	public static String createFile(ServerIDManager idManager, int storageNameId, DuplicateNode[] duplicateNodes) {
		StringBuilder builder = new StringBuilder();
		builder.append(UUID.randomUUID().toString().replaceAll("-", ""));
		
		for(DuplicateNode node : duplicateNodes) {
			String serverId = DuplicationEnvironment.VIRTUAL_SERVICE_GROUP.equals(node.getGroup()) ?
					node.getId() : idManager.getOtherSecondID(node.getId(), storageNameId);
					
			builder.append('_').append(serverId);
		}
		
		return builder.toString();
	}
}
