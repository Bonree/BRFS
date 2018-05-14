package com.bonree.brfs.duplication.coordinator;

import java.io.File;

import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.base.Splitter;

public class FilePathBuilder {
	
	public static String buildPath(FileNode file, String serviceId) {
		int index = 0;
		for(String id : Splitter.on("_").splitToList(file.getName())) {
			if(id.equals(serviceId)) {
				break;
			}
			
			index++;
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(File.separatorChar)
		.append(file.getStorageName())
		.append(File.separatorChar)
		.append(index)
		.append(File.separatorChar)
		.append(TimeUtils.timeInterval(file.getCreateTime(), 60 * 60 * 1000))
		.append(File.separatorChar)
		.append(file.getName());
		
		return builder.toString();
	}
}
