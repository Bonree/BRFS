package com.bonree.brfs.duplication;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidEncoder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.google.common.base.Splitter;

public class FidBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FidBuilder.class);
	
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
		
		Fid fid = builder.build();
		
		try {
			return FidEncoder.build(fid);
		} catch (Exception e) {
			LOG.error("error create FID: file[{}], offset[{}], size[{}]", node.getName(), offset, size, e);
		}
		
		return null;
	}
}
