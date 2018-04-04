package com.bonree.brfs.disknode.server;

import java.io.File;

public class DiskContext {
	private String rootDir;
	
	public DiskContext(String root) {
		this.rootDir = root;
	}
	
	public String localPath(String path) {
		return new File(rootDir, path).getAbsolutePath();
	}
}
