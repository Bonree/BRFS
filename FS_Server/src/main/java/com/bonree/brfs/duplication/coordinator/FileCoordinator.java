package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.duplication.utils.LifeCycle;

public interface FileCoordinator extends LifeCycle {
	boolean publish(FileNode node);
	boolean release(String fileName);
	
	void setFilePicker(FilePicker picker);
}
