package com.bonree.brfs.duplication.datastream;

public interface DataHandleCallback<T> {
	void completed(T result);
	void error(Throwable t);
}
