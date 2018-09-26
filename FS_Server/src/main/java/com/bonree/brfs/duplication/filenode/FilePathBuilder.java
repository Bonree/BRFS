package com.bonree.brfs.duplication.filenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.base.Splitter;

public class FilePathBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FilePathBuilder.class);
	
	private static final String PATH_SEPARATOR = "/";
	
	public static String buildFilePath(FileNode fileNode, String serviceId) {
		int index = 0;
		
		LOG.debug("build file path with sn[{}], serid[{}], time[{}] filename[{}]", fileNode.getStorageName(), serviceId, fileNode.getCreateTime(), fileNode.getName());
		for(String id : Splitter.on("_").splitToList(fileNode.getName())) {
			if(id.equals(serviceId)) {
				break;
			}
			
			index++;
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(PATH_SEPARATOR)
		.append(fileNode.getStorageName())
		.append(PATH_SEPARATOR)
		.append(index)
		.append(PATH_SEPARATOR)
		.append(TimeUtils.timeInterval(fileNode.getCreateTime(), fileNode.getTimeDuration()))
		.append(PATH_SEPARATOR)
		.append(fileNode.getName());
		
		return builder.toString();
	}
}
