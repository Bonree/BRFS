package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.base.Splitter;

public class FilePathBuilder {
	
	private static final String PATH_SEPARATOR = "/";
	
	public static String buildPath(FileNode file, String serviceId) {
		int index = 0;
		for(String id : Splitter.on("_").splitToList(file.getName())) {
			if(id.equals(serviceId)) {
				break;
			}
			
			index++;
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(PATH_SEPARATOR)
		.append(file.getStorageName())
		.append(PATH_SEPARATOR)
		.append(index)
		.append(PATH_SEPARATOR)
		.append(TimeUtils.timeInterval(file.getCreateTime(), 60 * 60 * 1000))
		.append(PATH_SEPARATOR)
		.append(file.getName());
		
		return builder.toString();
	}
}
