package com.br.disknode.server.handler;



public interface MessageHandler {
	void handle(DiskMessage msg, HandleResultCallback callback);
}
