package com.bonree.brfs.duplication.coordinator;

import java.util.List;

public interface FileNodeStorer {
	void save(FileNode fileNode) throws Exception;
	void delete(String fileName) throws Exception;
	FileNode getFileNode(String fileName) throws Exception;
	void update(FileNode fileNode) throws Exception;
	List<FileNode> listFileNodes(FileNodeFilter filter) throws Exception;
}
