package com.bonree.brfs.disknode.boot;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HttpConfig;
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
import com.bonree.brfs.disknode.server.handler.SequenceNumberCache;
import com.bonree.brfs.disknode.server.handler.WriteMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingBytesMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingMetaDataMessageHandler;
import com.bonree.brfs.disknode.server.handler.WritingSequenceMessageHandler;

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
		
		httpConfig.setBacklog(1024);
		server = new NettyHttpServer(httpConfig);
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler();
		requestHandler.addMessageHandler("PUT", new OpenMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(diskContext));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_DISK_NODE_ROOT, requestHandler);
		
		NettyHttpRequestHandler flushRequestHandler = new NettyHttpRequestHandler();
		flushRequestHandler.addMessageHandler("POST", new FlushMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_FLUSH_NODE_ROOT, flushRequestHandler);
		
		SequenceNumberCache cache = new SequenceNumberCache(writerManager);
		
		NettyHttpRequestHandler sequenceRequestHandler = new NettyHttpRequestHandler();
		sequenceRequestHandler.addMessageHandler("GET", new WritingSequenceMessageHandler(diskContext, cache));
		server.addContextHandler(DiskContext.URI_SEQUENCE_NODE_ROOT, sequenceRequestHandler);
		
		NettyHttpRequestHandler bytesRequestHandler = new NettyHttpRequestHandler();
		bytesRequestHandler.addMessageHandler("GET", new WritingBytesMessageHandler(diskContext, cache));
		server.addContextHandler(DiskContext.URI_SEQ_BYTE_NODE_ROOT, bytesRequestHandler);
		
		NettyHttpRequestHandler metaRequestHandler = new NettyHttpRequestHandler();
		metaRequestHandler.addMessageHandler("GET", new WritingMetaDataMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_META_NODE_ROOT, metaRequestHandler);
		
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler();
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_COPY_NODE_ROOT, cpRequestHandler);
		
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler();
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_LIST_NODE_ROOT, listRequestHandler);
		
		NettyHttpRequestHandler recoverRequestHandler = new NettyHttpRequestHandler();
		recoverRequestHandler.addMessageHandler("POST", new RecoveryMessageHandler(diskContext, serviceManager, writerManager, recorderManager));
		server.addContextHandler(DiskContext.URI_RECOVER_NODE_ROOT, recoverRequestHandler);
		
		NettyHttpRequestHandler pingRequestHandler = new NettyHttpRequestHandler();
		pingRequestHandler.addMessageHandler("GET", new PingPongRequestHandler());
		server.addContextHandler(DiskContext.URI_PING_PONG_ROOT, pingRequestHandler);
		
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
