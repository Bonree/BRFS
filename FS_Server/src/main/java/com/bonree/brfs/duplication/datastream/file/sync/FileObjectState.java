package com.bonree.brfs.duplication.datastream.file.sync;

public class FileObjectState {
	private final String serviceGroup;
	private final String serviceId;
	private final String filePath;
	private final long fileLength;
	
	public FileObjectState(String group, String id, String path, long length) {
		this.serviceGroup = group;
		this.serviceId = id;
		this.filePath = path;
		this.fileLength = length;
	}

	public String getServiceGroup() {
		return serviceGroup;
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getFilePath() {
		return filePath;
	}

	public long getFileLength() {
		return fileLength;
	}
}
