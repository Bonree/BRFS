package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.google.common.base.Splitter;

public class FilePathBuilder {
	
	private static final String PATH_SEPARATOR = "/";
	
	public static String buildFilePath(String storageName, String serviceId, long createTime, String fileName) {
		int index = 0;
		for(String id : Splitter.on("_").splitToList(fileName)) {
			if(id.equals(serviceId)) {
				break;
			}
			
			index++;
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(PATH_SEPARATOR)
		.append(storageName)
		.append(PATH_SEPARATOR)
		.append(index)
		.append(PATH_SEPARATOR)
		.append(TimeUtils.timeInterval(createTime, DuplicationEnvironment.DEFAULT_FILE_TIME_INTERVAL_MILLIS))
		.append(PATH_SEPARATOR)
		.append(fileName);
		
		return builder.toString();
	}
}
