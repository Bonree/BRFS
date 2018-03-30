package com.bonree.brfs.duplication.datastream.file;

public interface FileLounge {
	FileInfo getFileInfo(int storageNameId, int size) throws Exception;
	void deleteFile(FileInfo file);
}
