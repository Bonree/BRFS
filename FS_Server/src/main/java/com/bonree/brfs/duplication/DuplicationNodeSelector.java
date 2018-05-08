package com.bonree.brfs.duplication;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public interface DuplicationNodeSelector {
	DuplicateNode[] getDuplicationNodes(int nums);
}
