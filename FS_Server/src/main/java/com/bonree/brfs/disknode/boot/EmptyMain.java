package com.bonree.brfs.disknode.boot;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HttpConfig;
import com.bonree.brfs.common.http.netty.NettyHttpContextHandler;
import com.bonree.brfs.common.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.http.netty.NettyHttpServer;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.server.handler.CloseMessageHandler;
import com.bonree.brfs.disknode.server.handler.DeleteMessageHandler;
import com.bonree.brfs.disknode.server.handler.FileCopyMessageHandler;
import com.bonree.brfs.disknode.server.handler.FlushMessageHandler;
import com.bonree.brfs.disknode.server.handler.ListMessageHandler;
import com.bonree.brfs.disknode.server.handler.OpenMessageHandler;
import com.bonree.brfs.disknode.server.handler.PingPongRequestHandler;
import com.bonree.brfs.disknode.server.handler.ReadMessageHandler;
import com.bonree.brfs.disknode.server.handler.RecoveryMessageHandler;
import com.bonree.brfs.disknode.server.handler.WriteMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingInfoMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingMetaDataMessageHandler;

public class EmptyMain implements LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(EmptyMain.class);
	
	private NettyHttpServer server;
	private HttpConfig httpConfig;
	
	private DiskContext diskContext;
	
	private FileWriterManager writerManager;
	private ServiceManager serviceManager;
	
	public EmptyMain(ServerConfig serverConfig, ServiceManager serviceManager) {
		this(serverConfig.getDiskPort(), serverConfig.getDataPath(), serviceManager);
	}
	
	public EmptyMain(int port, String diskContextPath, ServiceManager serviceManager) {
		this.httpConfig = new HttpConfig(port);
		this.diskContext = new DiskContext(diskContextPath);
		this.serviceManager = serviceManager;
	}

	@Override
	public void start() throws Exception {
		LOG.info("Empty Main--port[{}]", httpConfig.getPort());
		
		checkDiskContextPath();
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		writerManager = new FileWriterManager(recorderManager, diskContext);
		writerManager.start();
		
		serviceManager.addServiceStateListener(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				LOG.info("service[{}] removed, time to flush all files", service);
				writerManager.flushAll();
			}
			
			@Override
			public void serviceAdded(Service service) {
			}
		});
		
		server = new NettyHttpServer(httpConfig);
		
		NettyHttpContextHandler contextHandler = new NettyHttpContextHandler(DiskContext.URI_DISK_NODE_ROOT);
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		requestHandler.addMessageHandler("PUT", new OpenMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(diskContext));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(diskContext, writerManager));
		contextHandler.setNettyHttpRequestHandler(requestHandler);
		server.addContextHandler(contextHandler);
		
		NettyHttpContextHandler flushHandler = new NettyHttpContextHandler(DiskContext.URI_FLUSH_NODE_ROOT);
		NettyHttpRequestHandler flushRequestHandler = new NettyHttpRequestHandler();
		flushRequestHandler.addMessageHandler("POST", new FlushMessageHandler(diskContext, writerManager));
		flushHandler.setNettyHttpRequestHandler(flushRequestHandler);
		server.addContextHandler(flushHandler);
		
		NettyHttpContextHandler infoHandler = new NettyHttpContextHandler(DiskContext.URI_INFO_NODE_ROOT);
		NettyHttpRequestHandler infoRequestHandler = new NettyHttpRequestHandler();
		infoRequestHandler.addMessageHandler("GET", new WritingInfoMessageHandler(diskContext, writerManager));
		infoHandler.setNettyHttpRequestHandler(infoRequestHandler);
		server.addContextHandler(infoHandler);
		
		NettyHttpContextHandler metaHandler = new NettyHttpContextHandler(DiskContext.URI_META_NODE_ROOT);
		NettyHttpRequestHandler metaRequestHandler = new NettyHttpRequestHandler();
		metaRequestHandler.addMessageHandler("GET", new WritingMetaDataMessageHandler(diskContext, writerManager));
		metaHandler.setNettyHttpRequestHandler(metaRequestHandler);
		server.addContextHandler(metaHandler);
		
		NettyHttpContextHandler cpHandler = new NettyHttpContextHandler(DiskContext.URI_COPY_NODE_ROOT);
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler();
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(diskContext));
		cpHandler.setNettyHttpRequestHandler(cpRequestHandler);
		server.addContextHandler(cpHandler);
		
		NettyHttpContextHandler listHandler = new NettyHttpContextHandler(DiskContext.URI_LIST_NODE_ROOT);
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler();
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(diskContext));
		listHandler.setNettyHttpRequestHandler(listRequestHandler);
		server.addContextHandler(listHandler);
		
		NettyHttpContextHandler recoverHandler = new NettyHttpContextHandler(DiskContext.URI_RECOVER_NODE_ROOT);
		NettyHttpRequestHandler recoverRequestHandler = new NettyHttpRequestHandler();
		recoverRequestHandler.addMessageHandler("POST", new RecoveryMessageHandler(diskContext, serviceManager, writerManager, recorderManager));
		recoverHandler.setNettyHttpRequestHandler(recoverRequestHandler);
		server.addContextHandler(recoverHandler);
		
		NettyHttpContextHandler pingHandler = new NettyHttpContextHandler(DiskContext.URI_PING_PONG_ROOT);
		NettyHttpRequestHandler pingRequestHandler = new NettyHttpRequestHandler();
		pingRequestHandler.addMessageHandler("GET", new PingPongRequestHandler());
		pingHandler.setNettyHttpRequestHandler(pingRequestHandler);
		server.addContextHandler(pingHandler);
		
		server.start();
	}
	
	private void checkDiskContextPath() {
		if(!new File(diskContext.getRootDir()).exists()) {
			throw new IllegalArgumentException("Disk context path[" + diskContext.getRootDir() + "] is not existed!");
		}
	}

	@Override
	public void stop() throws Exception {
		server.stop();
		writerManager.stop();
	}
}
