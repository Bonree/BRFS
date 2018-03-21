package com.br.disknode.server.jetty;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;

import com.br.disknode.DiskWriterManager;
import com.br.disknode.server.DiskOperation;
import com.br.disknode.server.handler.impl.CloseMessageHandler;
import com.br.disknode.server.handler.impl.DeleteMessageHandler;
import com.br.disknode.server.handler.impl.OpenMessageHandler;
import com.br.disknode.server.handler.impl.ReadMessageHandler;
import com.br.disknode.server.handler.impl.WriteMessageHandler;
import com.br.disknode.server.netty.DiskNettyHttpRequestHandler;
import com.br.disknode.server.netty.NettyHttpServer;
import com.br.disknode.server.netty.NettyHttpContextHandler;
import com.br.disknode.utils.InputUtils;

public class Test {

	public static void main(String[] args) throws Exception {
		int port = 8899;
		if(args.length > 0) {
			port = Integer.parseInt(args[0]);
		}
		
		JettyDiskNodeHttpServer s = new JettyDiskNodeHttpServer(port);
		
		DiskWriterManager nodeManager = new DiskWriterManager();
		nodeManager.start();
		DiskJettyHttpRequestHandler httpRequestHandler = new DiskJettyHttpRequestHandler();
		httpRequestHandler.put(DiskOperation.OP_OPEN, new OpenMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_WRITE, new WriteMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_READ, new ReadMessageHandler());
		httpRequestHandler.put(DiskOperation.OP_CLOSE, new CloseMessageHandler(nodeManager));
		httpRequestHandler.put(DiskOperation.OP_DELETE, new DeleteMessageHandler(nodeManager));
		
		ContextHandler handler = new ContextHandler("/disk");
		handler.setHandler(httpRequestHandler);
//		handler.setHandler(new AbstractHandler() {
//			
//			@Override
//			public void handle(String target, Request baseRequest,
//					HttpServletRequest request, HttpServletResponse response)
//					throws IOException, ServletException {
//				System.out.println("length=" + request.getContentLength());
//				int contentLength = request.getContentLength();
//				System.out.println("content length############" + contentLength);
//				byte[] data = new byte[Math.max(contentLength, 0)];
//				if(request.getContentLength() > 0) {
//					InputUtils.readBytes(request.getInputStream(), data, 0, data.length);
//					
//					System.out.println(new String(data));
//				}
//				baseRequest.setHandled(true);
//			}
//		});
		s.addContextHandler(handler);
		
		s.start();
		System.out.println("####################SERVER STARTED#####################");
	}

}
