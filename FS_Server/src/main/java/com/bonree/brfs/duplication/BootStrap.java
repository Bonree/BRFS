package com.bonree.brfs.duplication;

import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.duplication.datastream.handler.DeleteDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.DuplicationRequestHandler;
import com.bonree.brfs.duplication.datastream.handler.ReadDataMessageHandler;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessageHandler;

public class BootStrap {

	public static void main(String[] args) throws InterruptedException {
		NettyHttpServer httpServer = new NettyHttpServer(8899);
		
		DuplicationRequestHandler requestHandler = new DuplicationRequestHandler();
		requestHandler.addMessageHandler("POST", new WriteDataMessageHandler(null));
		requestHandler.addMessageHandler("GET", new ReadDataMessageHandler());
		requestHandler.addMessageHandler("DELETE", new DeleteDataMessageHandler());
		NettyHttpContextHandler contextHttpHandler = new NettyHttpContextHandler("/duplication");
		contextHttpHandler.setNettyHttpRequestHandler(requestHandler);
		httpServer.addContextHandler(contextHttpHandler);
		
		httpServer.start();
	}

}
