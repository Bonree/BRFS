package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.service.Service;

public interface FileNodeSink {
	Service getService();
	void fill(FileNode fileNode);
}
