package com.bonree.brfs.common.http.jetty;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import com.bonree.brfs.common.utils.LifeCycle;

/**
 * Jetty实现的Http Server启动类
 * 
 * @author chen
 *
 */
public class JettyDiskNodeHttpServer implements LifeCycle {
	private Server server;
	private ContextHandlerCollection hanlderList = new ContextHandlerCollection();
	
	public JettyDiskNodeHttpServer(int port) {
		this(null, port);
	}
	
	public JettyDiskNodeHttpServer(String host, int port) {
		this.server = new Server();
		ServerConnector connector = new ServerConnector(server);
		connector.setHost(host);
		connector.setPort(port);
		connector.setIdleTimeout(30000);
		
		server.addConnector(connector);
	}

	@Override
	public void start() throws Exception {
		server.setHandler(hanlderList);
		server.start();
	}

	@Override
	public void stop() throws Exception {
		server.stop();
	}
	
	public void addContextHandler(ContextHandler contextHandler) {
		hanlderList.addHandler(contextHandler);
	}
}
