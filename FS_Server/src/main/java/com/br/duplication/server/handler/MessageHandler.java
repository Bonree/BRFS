package com.br.duplication.server.handler;



public interface MessageHandler<T> {
	void handle(T msg, HandleResultCallback callback);
}
