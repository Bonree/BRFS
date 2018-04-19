package com.bonree.brfs.duplication.datastream.file;

import java.util.List;

public interface FileLounge {
	void addFileLimiter(FileLimiter file);
	FileLimiter getFileLimiter(int storageNameId, int size) throws Exception;
	List<FileLimiter> getAllFileLimiterList();
	boolean closeFile(FileLimiter file);
	void setFileCloseListener(FileCloseListener listener);
	
	public static interface FileCloseListener {
		boolean close(FileLimiter file);
	}
}
