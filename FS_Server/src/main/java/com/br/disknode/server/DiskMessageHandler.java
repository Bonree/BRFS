package com.br.disknode.server;

public interface DiskMessageHandler {
	void handle(DiskMessage msg, HandleResultCallback callback);
}
