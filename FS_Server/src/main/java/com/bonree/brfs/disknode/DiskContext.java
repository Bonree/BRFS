package com.bonree.brfs.disknode;

import java.io.File;

public class DiskContext {
	private String workDirectory;
	
	public DiskContext(String workDir) {
		this.workDirectory = workDir;
	}
	
	public String getAbsoluteFilePath(String path) {
		return new File(workDirectory, path).getAbsolutePath();
	}
}
