package com.bonree.brfs.duplication;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class MockDuplicationNodeSelector implements DuplicationNodeSelector {

	@Override
	public DuplicateNode[] getDuplicationNodes(int nums) {
		DuplicateNode[] nodes = new DuplicateNode[2];
		
		DuplicateNode node1 = new DuplicateNode();
		node1.setGroup("disk");
		node1.setId("disk_1");
		nodes[0] = node1;
		
		DuplicateNode node2 = new DuplicateNode();
		node2.setGroup("disk");
		node2.setId("disk_2");
		nodes[1] = node2;
		
		return nodes;
	}

}
