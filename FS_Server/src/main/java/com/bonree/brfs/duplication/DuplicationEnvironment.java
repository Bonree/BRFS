package com.bonree.brfs.duplication;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;

public final class DuplicationEnvironment {
	
	public static final String URI_DUPLICATION_NODE_ROOT = "/duplication";
	
	public static final String URI_STORAGENAME_NODE_ROOT = "/storageName";
	
	public static final int DEFAULT_FILE_HEADER_SIZE = 2;
	public static final int DEFAULT_FILE_TAILER_SIZE = 9;
	
	public static final int DEFAULT_MAX_AVAILABLE_FILE_SPACE = Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_FILE_CAPACITY)
			- DEFAULT_FILE_HEADER_SIZE - DEFAULT_FILE_TAILER_SIZE;
	
	public static final String VIRTUAL_SERVICE_GROUP = Configs.getConfiguration().GetConfig(DuplicateNodeConfigs.CONFIG_VIRTUAL_SERVICE_GROUP_NAME);
}
