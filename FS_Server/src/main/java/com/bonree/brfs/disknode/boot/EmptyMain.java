package com.bonree.brfs.disknode.boot;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.server.handler.CloseMessageHandler;
import com.bonree.brfs.disknode.server.handler.DeleteMessageHandler;
import com.bonree.brfs.disknode.server.handler.FileCopyMessageHandler;
import com.bonree.brfs.disknode.server.handler.ListMessageHandler;
import com.bonree.brfs.disknode.server.handler.ReadMessageHandler;
import com.bonree.brfs.disknode.server.handler.WriteMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingInfoMessageHandler;

public class EmptyMain {
	private static final int id = 1;
	private static final String dir = "disk_" + id;

	public static void main(String[] args) throws Exception {
		int port = Integer.parseInt("910" + id);
		System.out.println("----port---" + port);
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.101.86:2181", 3000, 15000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		client = client.usingNamespace("brfstest");
		
		ServiceManager serviceManager = new DefaultServiceManager(client);
		serviceManager.start();
		Service service = new Service("disk_" + id, "disk", "192.168.4.137", port);
		serviceManager.registerService(service);
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		
		HttpConfig config = new HttpConfig(port);
		
		NettyHttpServer server = new NettyHttpServer(config);
		
		NettyHttpContextHandler contextHandler = new NettyHttpContextHandler("/disk");
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		FileWriterManager writerManager = new FileWriterManager(recorderManager);
		writerManager.start();
		
		DiskContext context = new DiskContext("/root/temp/" + dir);
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(context, writerManager));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(context));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(context, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(context, writerManager));
		
		contextHandler.setNettyHttpRequestHandler(requestHandler);
		server.addContextHandler(contextHandler);
		
		NettyHttpContextHandler infoHandler = new NettyHttpContextHandler("/info");
		NettyHttpRequestHandler infoRequestHandler = new NettyHttpRequestHandler();
		infoRequestHandler.addMessageHandler("GET", new WritingInfoMessageHandler(context, recorderManager));
		infoHandler.setNettyHttpRequestHandler(infoRequestHandler);
		server.addContextHandler(infoHandler);
		
		NettyHttpContextHandler cpHandler = new NettyHttpContextHandler("/copy");
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler();
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(context));
		cpHandler.setNettyHttpRequestHandler(cpRequestHandler);
		server.addContextHandler(cpHandler);
		
		NettyHttpContextHandler listHandler = new NettyHttpContextHandler("/list");
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler();
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(context));
		listHandler.setNettyHttpRequestHandler(listRequestHandler);
		server.addContextHandler(listHandler);
		
		server.start();
	}

}
