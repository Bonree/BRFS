package com.bonree.brfs.disknode.boot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.server.handler.CloseMessageHandler;
import com.bonree.brfs.disknode.server.handler.DeleteMessageHandler;
import com.bonree.brfs.disknode.server.handler.FileCopyMessageHandler;
import com.bonree.brfs.disknode.server.handler.ListMessageHandler;
import com.bonree.brfs.disknode.server.handler.ReadMessageHandler;
import com.bonree.brfs.disknode.server.handler.RecoveryMessageHandler;
import com.bonree.brfs.disknode.server.handler.WriteMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingInfoMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingMetaDataMessageHandler;

public class EmptyMain implements LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(EmptyMain.class);
	
	private NettyHttpServer server;
	private FileWriterManager writerManager;
	private ServiceManager serviceManager;
	private ServerConfig serverConfig;
	
	public EmptyMain(ServerConfig serverConfig, ServiceManager serviceManager) {
		this.serverConfig = serverConfig;
		this.serviceManager = serviceManager;
	}

	@Override
	public void start() throws Exception {
		LOG.info("Empty Main--port[{}]", serverConfig.getDiskPort());
		
		DiskContext context = new DiskContext(serverConfig.getDataPath());
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		
		HttpConfig config = new HttpConfig(serverConfig.getDiskPort());
		
		server = new NettyHttpServer(config);
		
		NettyHttpContextHandler contextHandler = new NettyHttpContextHandler(DiskContext.URI_DISK_NODE_ROOT);
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		writerManager = new FileWriterManager(recorderManager, context);
		writerManager.start();
		
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(context, writerManager));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(context));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(context, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(context, writerManager));
		
		contextHandler.setNettyHttpRequestHandler(requestHandler);
		server.addContextHandler(contextHandler);
		
		NettyHttpContextHandler infoHandler = new NettyHttpContextHandler(DiskContext.URI_INFO_NODE_ROOT);
		NettyHttpRequestHandler infoRequestHandler = new NettyHttpRequestHandler();
		infoRequestHandler.addMessageHandler("GET", new WritingInfoMessageHandler(context, recorderManager));
		infoHandler.setNettyHttpRequestHandler(infoRequestHandler);
		server.addContextHandler(infoHandler);
		
		NettyHttpContextHandler metaHandler = new NettyHttpContextHandler(DiskContext.URI_META_NODE_ROOT);
		NettyHttpRequestHandler metaRequestHandler = new NettyHttpRequestHandler();
		metaRequestHandler.addMessageHandler("GET", new WritingMetaDataMessageHandler(context, recorderManager));
		metaHandler.setNettyHttpRequestHandler(metaRequestHandler);
		server.addContextHandler(metaHandler);
		
		NettyHttpContextHandler cpHandler = new NettyHttpContextHandler(DiskContext.URI_COPY_NODE_ROOT);
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler();
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(context));
		cpHandler.setNettyHttpRequestHandler(cpRequestHandler);
		server.addContextHandler(cpHandler);
		
		NettyHttpContextHandler listHandler = new NettyHttpContextHandler(DiskContext.URI_LIST_NODE_ROOT);
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler();
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(context));
		listHandler.setNettyHttpRequestHandler(listRequestHandler);
		server.addContextHandler(listHandler);
		
		NettyHttpContextHandler recoverHandler = new NettyHttpContextHandler(DiskContext.URI_RECOVER_NODE_ROOT);
		NettyHttpRequestHandler recoverRequestHandler = new NettyHttpRequestHandler();
		recoverRequestHandler.addMessageHandler("POST", new RecoveryMessageHandler(context, serviceManager, writerManager));
		recoverHandler.setNettyHttpRequestHandler(recoverRequestHandler);
		server.addContextHandler(recoverHandler);
		
		server.start();
	}

	@Override
	public void stop() throws Exception {
		writerManager.stop();
		server.stop();
	}
}
