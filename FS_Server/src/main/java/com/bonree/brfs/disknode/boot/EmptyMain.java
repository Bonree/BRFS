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
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileFormater;
import com.bonree.brfs.disknode.server.handler.CloseMessageHandler;
import com.bonree.brfs.disknode.server.handler.DeleteMessageHandler;
import com.bonree.brfs.disknode.server.handler.FileCopyMessageHandler;
import com.bonree.brfs.disknode.server.handler.FileLengthMessageHandler;
import com.bonree.brfs.disknode.server.handler.FlushMessageHandler;
import com.bonree.brfs.disknode.server.handler.ListMessageHandler;
import com.bonree.brfs.disknode.server.handler.OpenMessageHandler;
import com.bonree.brfs.disknode.server.handler.PingPongRequestHandler;
import com.bonree.brfs.disknode.server.handler.ReadMessageHandler;
import com.bonree.brfs.disknode.server.handler.RecoveryMessageHandler;
import com.bonree.brfs.disknode.server.handler.WriteMessageHandler;

public class EmptyMain implements LifeCycle {
	private static final Logger LOG = LoggerFactory.getLogger(EmptyMain.class);
	
	private NettyHttpServer server;
	private HttpConfig httpConfig;
	
	private DiskContext diskContext;
	
	private FileWriterManager writerManager;
	private ServiceManager serviceManager;
	
	private ExecutorService requestHandlerExecutor;
	
	public EmptyMain(ServiceManager serviceManager) {
		this.diskContext = new DiskContext(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT));
		this.serviceManager = serviceManager;
		
		int workerThreadNum = Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_IO_WORKER_NUM,
				String.valueOf(Runtime.getRuntime().availableProcessors())));
		this.httpConfig = HttpConfig.newBuilder()
				.setHost(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST))
				.setPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT))
				.setAcceptWorkerNum(1)
				.setRequestHandleWorkerNum(workerThreadNum)
				.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")))
				.build();
	}

	@Override
	public void start() throws Exception {
		LOG.info("Empty Main--port[{}]", httpConfig.getPort());
		
		checkDiskContextPath();
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		writerManager = new FileWriterManager(recorderManager);
		writerManager.start();
		
		writerManager.rebuildFileWriterbyDir(diskContext.getRootDir());
		
		server = new NettyHttpServer(httpConfig);
		requestHandlerExecutor = Executors.newFixedThreadPool(
				Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_REQUEST_HANDLER_NUM),
				new PooledThreadFactory("request_handler"));
		
		FileFormater fileFormater = new SimpleFileFormater(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY));
		
		NettyHttpRequestHandler requestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		requestHandler.addMessageHandler("PUT", new OpenMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("POST", new WriteMessageHandler(diskContext, writerManager, fileFormater));
		requestHandler.addMessageHandler("GET", new ReadMessageHandler(diskContext, fileFormater));
		requestHandler.addMessageHandler("CLOSE", new CloseMessageHandler(diskContext, writerManager));
		requestHandler.addMessageHandler("DELETE", new DeleteMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_DISK_NODE_ROOT, requestHandler);
		
		NettyHttpRequestHandler flushRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		flushRequestHandler.addMessageHandler("POST", new FlushMessageHandler(diskContext, writerManager));
		server.addContextHandler(DiskContext.URI_FLUSH_NODE_ROOT, flushRequestHandler);
		
		NettyHttpRequestHandler sequenceRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		sequenceRequestHandler.addMessageHandler("GET", new FileLengthMessageHandler(diskContext, writerManager, fileFormater));
		server.addContextHandler(DiskContext.URI_LENGTH_NODE_ROOT, sequenceRequestHandler);
		
		NettyHttpRequestHandler cpRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		cpRequestHandler.addMessageHandler("POST", new FileCopyMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_COPY_NODE_ROOT, cpRequestHandler);
		
		NettyHttpRequestHandler listRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		listRequestHandler.addMessageHandler("GET", new ListMessageHandler(diskContext));
		server.addContextHandler(DiskContext.URI_LIST_NODE_ROOT, listRequestHandler);
		
		NettyHttpRequestHandler recoverRequestHandler = new NettyHttpRequestHandler(requestHandlerExecutor);
		recoverRequestHandler.addMessageHandler("POST", new RecoveryMessageHandler(diskContext, serviceManager, writerManager));
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
		writerManager.stop();
		
		if(requestHandlerExecutor != null) {
			requestHandlerExecutor.shutdown();
		}
	}
}
