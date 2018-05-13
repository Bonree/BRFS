package com.bonree.brfs.duplication;

import java.util.List;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.server.utils.FidEncoder;
import com.google.common.base.Splitter;

public class FidBuilder {
	
	public static String getFid(FileNode node, int storageId, long offset, int size) {
		Fid.Builder builder = Fid.newBuilder()
				.setCompress(0)
				.setStorageNameCode(storageId)
				.setTime(node.getCreateTime())
				.setUuid(node.getName())
				.setOffset(offset)
				.setSize(size)
				.setReplica(node.getDuplicateNodes().length)
				.setVersion(0);
		
		List<String> nameParts = Splitter.on("_").splitToList(node.getName());
		for(int i = 0; i < nameParts.size(); i++) {
			builder.addServerId(Integer.parseInt(nameParts.get(i)));
		}
		
		try {
			return FidEncoder.build(builder.build());
		} catch (Exception e) {
		}
		
		return null;
	}
}
