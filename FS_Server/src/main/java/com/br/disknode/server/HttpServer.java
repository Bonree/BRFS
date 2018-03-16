package com.br.disknode.server;

import com.br.disknode.server.handler.HttpContextHandler;
import com.br.disknode.utils.LifeCycle;

public interface HttpServer extends LifeCycle {
	void addContextHandler(HttpContextHandler contextHandler);
}
