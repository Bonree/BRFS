package com.bonree.brfs.disknode.server.handler.data;

public class DeleteData {
	private boolean forceClose;
	private boolean recursive;
	
	public boolean isForceClose() {
		return forceClose;
	}

	public void setForceClose(boolean forceClose) {
		this.forceClose = forceClose;
	}

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}
}
