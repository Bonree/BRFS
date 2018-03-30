package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.utils.LifeCycle;

public interface FileNodeSinkManager extends LifeCycle {
	void registerFileNodeSink(FileNodeSink sink) throws Exception;
}
