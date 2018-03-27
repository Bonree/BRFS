package com.bonree.brfs.duplication.coordinator;

import java.util.List;

public interface FileNodeStorer {
	void save(FileNode fileNode) throws Exception;
	void delete(String fileName) throws Exception;
	FileNode getFileNode(String fileName) throws Exception;
	void update(String fileName, FileNode fileNode) throws Exception;
	List<FileNode> listFileNodes() throws Exception;
}
