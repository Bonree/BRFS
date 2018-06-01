package com.bonree.brfs.duplication;

public final class DuplicationEnvironment {
	
	public static final String URI_DUPLICATION_NODE_ROOT = "/duplication";
	
	public static final String URI_STORAGENAME_NODE_ROOT = "/storageName";
	
	public static final int DEFAULT_MAX_FILE_SIZE = 64 * 1024 * 1024;
	public static final int DEFAULT_FILE_HEADER_SIZE = 2;
	public static final int DEFAULT_FILE_TAILER_SIZE = 9;
	
	public static final long DEFAULT_FILE_TIME_INTERVAL_MILLIS = 60 * 60 * 1000;
	
	public static final int DEFAULT_MAX_AVAILABLE_FILE_SPACE = DEFAULT_MAX_FILE_SIZE - DEFAULT_FILE_HEADER_SIZE - DEFAULT_FILE_TAILER_SIZE;
	
	public static final String VIRTUAL_SERVICE_GROUP = "virtual_service_group";
}
