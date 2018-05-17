package com.bonree.brfs.duplication;

import java.util.List;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidEncoder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.google.common.base.Splitter;

public class FidBuilder {
	
	public static String getFid(FileNode node, long offset, int size) {
		Fid.Builder builder = Fid.newBuilder()
				.setCompress(0)
				.setStorageNameCode(node.getStorageId())
				.setTime(node.getCreateTime())
				.setOffset(offset)
				.setSize(size)
				.setReplica(node.getDuplicateNodes().length)
				.setVersion(0);
		
		List<String> nameParts = Splitter.on("_").splitToList(node.getName());
		builder.setUuid(nameParts.get(0));
		for(int i = 1; i < nameParts.size(); i++) {
			builder.addServerId(Integer.parseInt(nameParts.get(i)));
		}
		
		try {
			return FidEncoder.build(builder.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
