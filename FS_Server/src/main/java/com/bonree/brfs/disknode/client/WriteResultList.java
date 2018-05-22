package com.bonree.brfs.disknode.client;

import com.bonree.brfs.disknode.server.handler.data.WriteResult;

public class WriteResultList {
	private WriteResult[] writeResults;

	public WriteResult[] getWriteResults() {
		return writeResults;
	}

	public void setWriteResults(WriteResult[] writeResults) {
		this.writeResults = writeResults;
	}

}
