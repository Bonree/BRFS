package com.bonree.brfs.duplication;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidDecoder;
import com.bonree.brfs.common.write.data.FidEncoder;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FidBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FidBuilder.class);
	
	public static String getFid(FileNode node, long offset, int size) {
		Fid.Builder builder = Fid.newBuilder()
				.setVersion(0)
				.setCompress(0)
				.setStorageNameCode(node.getStorageId())
				.setTime(node.getCreateTime())
				.setDuration(node.getTimeDurationMillis())
				.setOffset(offset)
				.setSize(size);
		
		List<String> nameParts = Splitter.on("_").splitToList(node.getName());
		builder.setUuid(nameParts.get(0));
		for(int i = 1; i < nameParts.size(); i++) {
			builder.addServerId(nameParts.get(i));
		}
		
		Fid fid = builder.build();
		
		try {
			return FidEncoder.build(fid);
		} catch (Exception e) {
			LOG.error("error create FID: file[{}], offset[{}], size[{}]", node.getName(), offset, size, e);
		}
		
		return null;
	}

	public static String setFileType(String oldfid) throws Exception {
		Fid oldFidProto = FidDecoder.build(oldfid);
		Fid.Builder builder = Fid.newBuilder()
				.setVersion(oldFidProto.getVersion())
				.setCompress(oldFidProto.getCompress())
				.setStorageNameCode(oldFidProto.getStorageNameCode())
				.setTime(oldFidProto.getTime())
				.setDuration(oldFidProto.getDuration())
				.setOffset(oldFidProto.getOffset())
				.setSize(oldFidProto.getSize())
				.setIsBigFile(true);

		builder.setUuid(oldFidProto.getUuid());
		for(int i = 0; i < oldFidProto.getServerIdList().size(); i++) {
			builder.addServerId(oldFidProto.getServerIdList().get(i));
		}

		Fid fid = builder.build();

		try {
			return FidEncoder.build(fid);
		} catch (Exception e) {
			LOG.error("error set big file flag FID: [{}]", fid);
		}

		return null;
	}
}
