package com.bonree.brfs.disknode;

public interface InputEventCallback {
	void complete(InputResult result);
	void error(Throwable t);
}
