package com.bonree.brfs.client.utils;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;

public final class FilePathBuilder {
	
	private static final String PATH_SEPARATOR = "/";
	
	public static String buildPath(Fid fid, String timeInterval, String storageName, int index) {
		StringBuilder builder = new StringBuilder();
		builder.append(PATH_SEPARATOR)
		.append(storageName)
		.append(PATH_SEPARATOR)
		.append(index)
		.append(PATH_SEPARATOR)
		.append(timeInterval)
		.append(PATH_SEPARATOR)
		.append(fid.getUuid().toLowerCase());
		
		for(String serverId : fid.getServerIdList()) {
			builder.append("_").append(serverId);
		}
		
		return builder.toString();
	}
}
