package com.br.disknode.server.jetty;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.slf4j.LoggerFactory;

import com.br.disknode.DiskNodeManager;
import com.br.disknode.WriteWorker;
import com.br.disknode.watch.WatchListener;
import com.br.disknode.watch.WatchMarket;

public class DiskServer {
	
	public static void main(String[] args) throws Exception {
		Server server = new Server();
		int port = 8080;
		
		if(args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		
		ServerConnector connector = new ServerConnector(server);
		connector.setPort(port);
		connector.setIdleTimeout(30000);
		
		server.addConnector(connector);
		
		ContextHandler context = new ContextHandler();
		context.setContextPath("/disk");
		
		DiskNodeManager nodeManager = new DiskNodeManager();
		nodeManager.start();
		context.setHandler(new DiskHandler(nodeManager));
		
		server.setHandler(context);
		
		server.start();
		
		WatchMarket.get().start();
		
		server.join();
		
		nodeManager.stop();
	}
}
