package com.bonree.brfs.common.net.tcp.file;

public interface ReadObjectTranslator {
	String filePath(String path);
	long offset(long offset);
	int length(int length);
}
