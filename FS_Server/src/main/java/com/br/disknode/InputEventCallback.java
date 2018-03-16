package com.br.disknode;

public interface InputEventCallback {
	void complete(InputResult result);
	void completeError(Throwable t);
}
