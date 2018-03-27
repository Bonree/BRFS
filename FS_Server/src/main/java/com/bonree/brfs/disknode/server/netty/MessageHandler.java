package com.bonree.brfs.disknode.server.netty;

import com.bonree.brfs.disknode.server.handler.HandleResultCallback;

public interface MessageHandler<T> {
	void handle(T msg, HandleResultCallback callback);
}
