package com.bonree.brfs.duplication.synchronize;

import java.util.concurrent.TimeUnit;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.duplication.coordinator.FileNode;

public interface FileSynchronizer extends LifeCycle{
	void synchronize(FileNode fileNode, FileSynchronizeCallback callback, long delayedTime, TimeUnit unit);
	void synchronize(FileNode fileNode, FileSynchronizeCallback callback);
}
