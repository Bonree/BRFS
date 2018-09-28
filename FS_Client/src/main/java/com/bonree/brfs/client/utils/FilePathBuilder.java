package com.bonree.brfs.client.utils;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.utils.TimeUtils;

public final class FilePathBuilder {
	
	private static final String PATH_SEPARATOR = "/";
	
	public static String buildPath(Fid fid, String storageName, int index) {
		StringBuilder builder = new StringBuilder();
		builder.append(PATH_SEPARATOR)
		.append(storageName)
		.append(PATH_SEPARATOR)
		.append(index)
		.append(PATH_SEPARATOR)
		.append(TimeUtils.timeInterval(fid.getTime(), fid.getDuration()))
		.append(PATH_SEPARATOR)
		.append(fid.getUuid().toLowerCase());
		
		for(String serverId : fid.getServerIdList()) {
			builder.append("_").append(serverId);
		}
		
		return builder.toString();
	}
}
