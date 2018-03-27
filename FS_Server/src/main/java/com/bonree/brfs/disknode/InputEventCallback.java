package com.bonree.brfs.disknode;

public interface InputEventCallback {
	void complete(InputResult result);
	void completeError(Throwable t);
}
