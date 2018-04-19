package com.bonree.brfs.duplication.recovery;

import java.util.BitSet;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class FileLack {
	private BitSet lackSequence;
	private DuplicateNode node;

	public BitSet getLackSequence() {
		return lackSequence;
	}

	public void setLackSequence(BitSet lackSequence) {
		this.lackSequence = lackSequence;
	}

	public DuplicateNode getNode() {
		return node;
	}

	public void setNode(DuplicateNode node) {
		this.node = node;
	}
}
