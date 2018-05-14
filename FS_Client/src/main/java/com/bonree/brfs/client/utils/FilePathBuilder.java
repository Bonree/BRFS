package com.bonree.brfs.client.utils;

import java.io.File;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.utils.TimeUtils;

public final class FilePathBuilder {
	
	public static String buildPath(Fid fid, String storageName, int index) {
		StringBuilder builder = new StringBuilder();
		builder.append(File.separatorChar)
		.append(storageName)
		.append(File.separatorChar)
		.append(index)
		.append(File.separatorChar)
		.append(TimeUtils.timeInterval(fid.getTime(), 60 * 60 * 1000))
		.append(File.separatorChar)
		.append(fid.getUuid().toLowerCase());
		
		for(Integer serverId : fid.getServerIdList()) {
			builder.append("_").append(serverId);
		}
		
		return builder.toString();
	}
}
