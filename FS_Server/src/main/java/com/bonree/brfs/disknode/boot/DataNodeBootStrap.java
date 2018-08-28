package com.bonree.brfs.disknode.boot;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bonree.brfs.common.net.tcp.MessageChannelInitializer;
import com.bonree.brfs.common.net.tcp.ServerConfig;
import com.bonree.brfs.common.net.tcp.TcpServer;
import com.bonree.brfs.common.net.tcp.file.FileChannelInitializer;
import com.bonree.brfs.common.net.tcp.file.ReadObjectTranslator;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderGroup;
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
import com.bonree.brfs.disknode.server.tcp.handler.CloseFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.DeleteFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.FileRecoveryMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.FlushFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.ListFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.MetadataFetchMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.OpenFileMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.PingPongMessageHandler;
import com.bonree.brfs.disknode.server.tcp.handler.WriteFileMessageHandler;

public class DataNodeBootStrap implements LifeCycle {
	public static final int TYPE_OPEN_FILE = 0;
	public static final int TYPE_WRITE_FILE = 1;
	public static final int TYPE_CLOSE_FILE = 2;
	public static final int TYPE_DELETE_FILE = 3;
	public static final int TYPE_PING_PONG = 4;
	public static final int TYPE_FLUSH_FILE = 5;
	public static final int TYPE_METADATA = 6;
	public static final int TYPE_LIST_FILE = 7;
	public static final int TYPE_RECOVER_FILE = 8;
	
	private DiskContext diskContext;
	
	private FileWriterManager writerManager;
	private ServiceManager serviceManager;
	
	private TcpServer server;
	private TcpServer fileServer;
	private ExecutorService threadPool;
	private AsyncFileReaderGroup readerGroup;
	private ExecutorService fileExecutor;
	
	public DataNodeBootStrap(ServiceManager serviceManager) {
		this.diskContext = new DiskContext(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT));
		this.serviceManager = serviceManager;
		this.readerGroup = new AsyncFileReaderGroup(Math.min(2, Runtime.getRuntime().availableProcessors() / 2));
	}
	
	@Override
	public void start() throws Exception {
		createRootDirIfNeeded(diskContext.getRootDir());
		
		RecordCollectionManager recorderManager = new RecordCollectionManager();
		writerManager = new FileWriterManager(recorderManager, new FileValidChecker());
		writerManager.start();
		
		writerManager.rebuildFileWriterbyDir(diskContext.getRootDir());
		
		FileFormater fileFormater = new SimpleFileFormater(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY));
		
		threadPool = Executors.newFixedThreadPool(
				Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_REQUEST_HANDLER_NUM),
				new PooledThreadFactory("message_handler"));
		
		MessageChannelInitializer initializer = new MessageChannelInitializer(threadPool);
		initializer.addMessageHandler(TYPE_OPEN_FILE, new OpenFileMessageHandler(diskContext, writerManager));
		initializer.addMessageHandler(TYPE_WRITE_FILE, new WriteFileMessageHandler(diskContext, writerManager, fileFormater));
		initializer.addMessageHandler(TYPE_CLOSE_FILE, new CloseFileMessageHandler(diskContext, writerManager, fileFormater));
		initializer.addMessageHandler(TYPE_DELETE_FILE, new DeleteFileMessageHandler(diskContext, writerManager));
		initializer.addMessageHandler(TYPE_PING_PONG, new PingPongMessageHandler());
		initializer.addMessageHandler(TYPE_FLUSH_FILE, new FlushFileMessageHandler(diskContext, writerManager));
		initializer.addMessageHandler(TYPE_METADATA, new MetadataFetchMessageHandler(diskContext, writerManager, fileFormater));
		initializer.addMessageHandler(TYPE_LIST_FILE, new ListFileMessageHandler(diskContext));
		
		initializer.addMessageHandler(TYPE_RECOVER_FILE, new FileRecoveryMessageHandler(diskContext, serviceManager, writerManager, fileFormater, readerGroup));
		
		int workerThreadNum = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_SERVER_IO_NUM);
		
		ServerConfig config = new ServerConfig();
		config.setBacklog(Integer.parseInt(System.getProperty(SystemProperties.PROP_NET_BACKLOG, "2048")));
		config.setBossThreadNums(1);
		config.setWorkerThreadNums(workerThreadNum);
		config.setPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PORT));
		config.setHost(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST));
		
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(config.getWorkerThreadNums());
		
		server = new TcpServer(config, initializer, bossGroup, workerGroup);
		server.start();
		
		config.setPort(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_PORT));
		fileExecutor = Executors.newFixedThreadPool(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_READER_NUM),
				new PooledThreadFactory("file_reader"));
		FileChannelInitializer fileInitializer = new FileChannelInitializer(new ReadObjectTranslator() {
			
			@Override
			public long offset(long offset) {
				return fileFormater.absoluteOffset(offset);
			}
			
			@Override
			public int length(int length) {
				return length;
			}
			
			@Override
			public String filePath(String path) {
				return diskContext.getConcreteFilePath(path);
			}
			
		}, fileExecutor);
		
		fileServer = new TcpServer(config, fileInitializer);
		fileServer.start();
	}
	
	private void createRootDirIfNeeded(String dirPath) {
		File dir = new File(dirPath);
		if(!dir.exists()) {
			dir.mkdirs();
		}
	}

	@Override
	public void stop() throws Exception {
		if(fileServer != null) {
			fileServer.stop();
		}
		
		if(server != null) {
			server.stop();
		}
		
		writerManager.stop();
		
		if(threadPool != null) {
			threadPool.shutdown();
		}
		
		readerGroup.close();
		
		fileExecutor.shutdown();
	}

}
