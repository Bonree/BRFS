package com.bonree.brfs.disknode.boot;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HttpConfig;
import com.bonree.brfs.common.net.http.netty.NettyHttpRequestHandler;
import com.bonree.brfs.common.net.http.netty.NettyHttpServer;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
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
	
	private ExecutorService requestHandlerExecutor;
	
	private static final String DISKNODE_SERVICE_GROUP = Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME);
	private ServiceStateListener serviceStateListener;
	
	public EmptyMain(ServiceManager serviceManager) {
		this.diskContext = new DiskContext(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_DATA_ROOT));
		this.serviceManager = serviceManager;
		
		int workerThreadNum = Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_IO_WORKER_NUM,
				String.valueOf(Runtime.getRuntime().availableProcessors())));
		this.httpConfig = HttpConfig.newBuilder()
				.setHost(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_HOST))
				.setPort(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_PORT))
				.setAcceptWorkerNum(1)
				.setRequestHandleWorkerNum(workerThreadNum)
				.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")))
				.build();
		
		this.serviceStateListener = new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				LOG.info("service[{}] removed, time to flush all files", service);
				writerManager.flushAll();
			}
			
			@Override
			public void serviceAdded(Service service) {
			}
		};
	}

	@Override
	public void start() throws Exception {
		LOG.info("Empty Main--port[{}]", httpConfig.getPort());
		
		checkDiskContextPath();
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		writerManager = new FileWriterManager(recorderManager);
		writerManager.start();
		
		writerManager.rebuildFileWriterbyDir(diskContext.getRootDir());
		
		serviceManager.addServiceStateListener(DISKNODE_SERVICE_GROUP, serviceStateListener);
		
		server = new NettyHttpServer(httpConfig);
		requestHandlerExecutor = Executors.newFixedThreadPool(
				Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_REQUEST_HANDLER_NUM),
				new PooledThreadFactory("request_handler"));
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		requestHandler.addMessageHandler("PUT", new OpenMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(diskContext));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_DISK_NODE_ROOT, requestHandler);
		
		NettyHttpRequestHandler flushRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		flushRequestHandler.addMessageHandler("POST", new FlushMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_FLUSH_NODE_ROOT, flushRequestHandler);
		
		SequenceNumberCache cache = new SequenceNumberCache(writerManager);
		
		NettyHttpRequestHandler sequenceRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		sequenceRequestHandler.addMessageHandler("GET", new WritingSequenceMessageHandler(diskContext, cache));
		server.addContextHandler(DiskContext.URI_SEQUENCE_NODE_ROOT, sequenceRequestHandler);
		
		NettyHttpRequestHandler bytesRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		bytesRequestHandler.addMessageHandler("GET", new WritingBytesMessageHandler(diskContext, cache));
		server.addContextHandler(DiskContext.URI_SEQ_BYTE_NODE_ROOT, bytesRequestHandler);
		
		NettyHttpRequestHandler metaRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		metaRequestHandler.addMessageHandler("GET", new WritingMetaDataMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_META_NODE_ROOT, metaRequestHandler);
		
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_COPY_NODE_ROOT, cpRequestHandler);
		
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_LIST_NODE_ROOT, listRequestHandler);
		
		NettyHttpRequestHandler recoverRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		recoverRequestHandler.addMessageHandler("POST", new RecoveryMessageHandler(diskContext, serviceManager, writerManager, recorderManager));
		server.addContextHandler(DiskContext.URI_RECOVER_NODE_ROOT, recoverRequestHandler);
		
		NettyHttpRequestHandler pingRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
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
		serviceManager.removeServiceStateListener(DISKNODE_SERVICE_GROUP, serviceStateListener);
		writerManager.stop();
		
		if(requestHandlerExecutor != null) {
			requestHandlerExecutor.shutdown();
		}
	}
}
