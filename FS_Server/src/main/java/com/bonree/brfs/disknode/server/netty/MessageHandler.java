package com.br.disknode.server.netty;

import com.br.disknode.server.handler.HandleResultCallback;

public interface MessageHandler<T> {
	void handle(T msg, HandleResultCallback callback);
}
