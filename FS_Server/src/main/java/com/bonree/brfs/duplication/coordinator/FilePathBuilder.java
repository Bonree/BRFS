package com.bonree.brfs.duplication.coordinator;

public class FilePathBuilder {
	
	public static String buildPath(FileNode file) {
		return "/" + file.getName();
	}
}
