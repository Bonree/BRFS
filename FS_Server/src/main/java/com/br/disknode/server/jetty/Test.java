package com.br.disknode.server.jetty;

import org.eclipse.jetty.server.handler.ContextHandler;

import com.br.disknode.DiskWriterManager;
import com.br.disknode.server.DiskOperation;
import com.br.disknode.server.handler.impl.CloseMessageHandler;
import com.br.disknode.server.handler.impl.DeleteMessageHandler;
import com.br.disknode.server.handler.impl.OpenMessageHandler;
import com.br.disknode.server.handler.impl.ReadMessageHandler;
import com.br.disknode.server.handler.impl.WriteMessageHandler;
import com.br.disknode.server.netty.DiskHttpRequestHandler;
import com.br.disknode.server.netty.NettyDiskNodeHttpServer;
import com.br.disknode.server.netty.NettyHttpContextHandler;

public class Test {

	public static void main(String[] args) throws Exception {
		int port = 8899;
		if(args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		
		JettyDiskNodeHttpServer s = new JettyDiskNodeHttpServer(port);
		
		DiskWriterManager nodeManager = new DiskWriterManager();
		nodeManager.start();
		JettyHttpRequestHandler httpRequestHandler = new JettyHttpRequestHandler();
		httpRequestHandler.put(DiskOperation.OP_OPEN, new OpenMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_WRITE, new WriteMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_READ, new ReadMessageHandler());
		httpRequestHandler.put(DiskOperation.OP_CLOSE, new CloseMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_DELETE, new DeleteMessageHandler(nodeManager));
		
		ContextHandler handler = new ContextHandler("/disk");
		handler.setHandler(httpRequestHandler);
		s.addContextHandler(handler);
		
		s.start();
		System.out.println("####################SERVER STARTED#####################");
	}

}
