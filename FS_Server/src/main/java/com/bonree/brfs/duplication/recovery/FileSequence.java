package com.bonree.brfs.duplication.recovery;

import java.util.BitSet;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class FileSequence {
	private DuplicateNode node;
	private BitSet sequenceSet;

	public DuplicateNode getNode() {
		return node;
	}

	public void setNode(DuplicateNode node) {
		this.node = node;
	}

	public BitSet getSequenceSet() {
		return sequenceSet;
	}

	public void setSequenceSet(BitSet sequenceSet) {
		this.sequenceSet = sequenceSet;
	}
}
