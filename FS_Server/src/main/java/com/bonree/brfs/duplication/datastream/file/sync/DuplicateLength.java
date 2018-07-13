package com.bonree.brfs.duplication.datastream.file.sync;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class DuplicateLength {
	private DuplicateNode node;
	private long fileLength;

	public DuplicateNode getNode() {
		return node;
	}

	public void setNode(DuplicateNode node) {
		this.node = node;
	}

	public long getFileLength() {
		return fileLength;
	}

	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}
}
