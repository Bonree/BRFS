package com.bonree.brfs.duplication.coordinator;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
import com.google.common.base.Splitter;

public class FilePathBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FilePathBuilder.class);
	
	private static final String PATH_SEPARATOR = "/";
	
	private static final long TIME_PATITION_INTERVAL_MILLIES = TimeUnit.MINUTES.toMillis(Configs.getConfiguration()
			.GetConfig(DuplicateNodeConfigs.CONFIG_FILE_PATITION_INTERVAL_MINUTES));
	
	public static String buildFilePath(String storageName, String serviceId, long createTime, String fileName) {
		int index = 0;
		
		LOG.info("build file path with sn[{}], serid[{}], time[{}] filename[{}]", storageName, serviceId, createTime, fileName);
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
		.append(TimeUtils.timeInterval(createTime, TIME_PATITION_INTERVAL_MILLIES))
		.append(PATH_SEPARATOR)
		.append(fileName);
		
		return builder.toString();
	}
}
