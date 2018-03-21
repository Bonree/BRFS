package com.br.duplication.coordinator;

import com.br.duplication.utils.LifeCycle;

public interface FileCoordinator extends LifeCycle {
	boolean publish(FileNode node);
	void release(FileNode node);
}
