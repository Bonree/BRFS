package com.br.disknode.server;

public interface DiskNodeServer {
	void addHandler(String op, DiskMessageHandler handler);
}
