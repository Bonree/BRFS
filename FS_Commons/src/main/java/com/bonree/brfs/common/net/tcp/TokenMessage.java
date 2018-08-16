package com.bonree.brfs.common.net.tcp;

public interface TokenMessage<T> {
	int messageToken();
	T message();
}
