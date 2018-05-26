package com.bonree.brfs.duplication.coordinator.zk;

import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeFilter;

public class IdentityFileNodeFilter implements FileNodeFilter {

	@Override
	public boolean filter(FileNode fileNode) {
		return true;
	}

}
