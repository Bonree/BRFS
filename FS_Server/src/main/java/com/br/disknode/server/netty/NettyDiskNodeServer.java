package com.br.disknode.server.netty;

import java.util.HashMap;
import java.util.Map;

import com.br.disknode.server.DiskMessageHandler;
import com.br.disknode.server.DiskNodeServer;

public class NettyDiskNodeServer implements DiskNodeServer {
	private Map<String, DiskMessageHandler> handlers = new HashMap<String, DiskMessageHandler>();

	@Override
	public void addHandler(String op, DiskMessageHandler handler) {
		
	}

}
