package com.bonree.brfs.duplication.datastream.file;

import java.util.List;

public interface FileLounge {
	FileLimiter getFileInfo(int storageNameId, int size) throws Exception;
	List<FileLimiter> getFileLimiterList();
	void deleteFile(FileLimiter file);
}
