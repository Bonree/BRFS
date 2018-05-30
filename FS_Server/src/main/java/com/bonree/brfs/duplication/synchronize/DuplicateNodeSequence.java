package com.bonree.brfs.duplication.synchronize;

import java.util.BitSet;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class DuplicateNodeSequence {
	private DuplicateNode node;
	private BitSet sequenceNumbers;

	public DuplicateNode getNode() {
		return node;
	}

	public void setNode(DuplicateNode node) {
		this.node = node;
	}

	public BitSet getSequenceNumbers() {
		return sequenceNumbers;
	}

	public void setSequenceNumbers(BitSet sequenceNumber) {
		this.sequenceNumbers = sequenceNumber;
	}
}
