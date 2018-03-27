package com.bonree.brfs.duplication.data;

public interface DataHandleCallback<T> {
	void completed(T result);
	void error(Throwable t);
}
