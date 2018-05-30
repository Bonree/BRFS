package com.bonree.brfs.duplication.recovery;

import com.bonree.brfs.duplication.coordinator.FileNode;

public interface FileSynchronizeCallback {
	void complete(FileNode file);
	void error(Throwable cause);
}
