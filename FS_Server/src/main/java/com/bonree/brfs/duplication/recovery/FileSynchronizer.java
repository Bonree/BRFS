package com.bonree.brfs.duplication.recovery;

import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.duplication.coordinator.FileNode;

public interface FileSynchronizer extends LifeCycle{
	void synchronize(FileNode fileNode, FileSynchronizeCallback listener);
}
