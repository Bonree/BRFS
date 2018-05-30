package com.bonree.brfs.duplication.synchronize;

import com.bonree.brfs.duplication.coordinator.FileNode;

public interface FileSynchronizeCallback {
	void complete(FileNode file);
	void error(Throwable cause);
}
