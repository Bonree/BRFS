package com.bonree.brfs.duplication.datastream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncExecutor;
import com.bonree.brfs.common.asynctask.AsyncTaskGroup;
import com.bonree.brfs.common.asynctask.AsyncTaskGroupCallback;
import com.bonree.brfs.common.asynctask.AsyncTaskResult;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeInvalidListener;
import com.bonree.brfs.duplication.coordinator.FileNodeSink;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileLimiter;
import com.bonree.brfs.duplication.datastream.file.FileLimiterCloser;
import com.bonree.brfs.duplication.datastream.file.FileLounge;
import com.bonree.brfs.duplication.datastream.file.FileLoungeCleaner;
import com.bonree.brfs.duplication.datastream.file.FileLoungeFactory;
import com.bonree.brfs.duplication.datastream.tasks.MultiDataWriteTask;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.duplication.synchronize.FileSynchronizeCallback;
import com.bonree.brfs.duplication.synchronize.FileSynchronizer;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DuplicateWriter {
	private static final Logger LOG = LoggerFactory.getLogger(DuplicateWriter.class);
	
	private static final int DEFAULT_MULTI_TASK_THREAD_NUM = 5;
	private AsyncExecutor multiTaskExecutor = new AsyncExecutor(DEFAULT_MULTI_TASK_THREAD_NUM);
	private static final int DEFAULT_DATA_WRITE_THREAD_NUM = 10;
	private AsyncExecutor writeTaskExecutor = new AsyncExecutor(DEFAULT_DATA_WRITE_THREAD_NUM);
	private static final int DEFAULT_RESULT_HANDLE_THREAD_NUM = 8;
	private ExecutorService resultExecutor = Executors.newFixedThreadPool(DEFAULT_RESULT_HANDLE_THREAD_NUM);
	private ScheduledExecutorService timedExecutor = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("File_Cleaner"));
	
	private static final long DEFAULT_CLEAN_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);
	private final long cleanFrequencyMillis = DEFAULT_CLEAN_FREQUENCY_MILLIS;
	
	private FileCoordinator fileCoordinator;
	private Service service;
	private DiskNodeConnectionPool connectionPool;
	
	private FileLoungeFactory fileLoungeFactory;
	private ConcurrentHashMap<Integer, FileLounge> fileLoungeList = new ConcurrentHashMap<Integer, FileLounge>();
	private List<ScheduledFuture<?>> fileLoungeCleaners = new ArrayList<ScheduledFuture<?>>();
	
	private FileSynchronizer fileRecovery;
	private ServerIDManager idManager;
	
	private FileLimiterCloser fileCloser;
	
	public DuplicateWriter(Service service, FileLoungeFactory fileLoungeFactory,
			FileCoordinator fileCoordinator, FileSynchronizer fileRecovery,
			ServerIDManager idManager, DiskNodeConnectionPool connectionPool,
			FileLimiterCloser fileCloser, StorageNameManager storageNameManager) {
		this.service = service;
		this.fileLoungeFactory = fileLoungeFactory;
		this.fileRecovery = fileRecovery;
		this.idManager = idManager;
		this.connectionPool = connectionPool;
		this.fileCloser = fileCloser;
		this.fileCoordinator = fileCoordinator;
		this.fileCoordinator.setFileNodeCleanListener(new FileInvalidator());
		
		try {
			fileCoordinator.addFileNodeSink(new DefaultFileNodeSink());
			
			storageNameManager.addStorageNameStateListener(new FileLoungeHandler());
		} catch (Exception e) {
			throw new RuntimeException("can not register FileNodeSink");
		}
	}
	
	private FileLounge getFileLoungeByStorageId(int storageId) {
		FileLounge fileLounge = fileLoungeList.get(storageId);
		if(fileLounge == null) {
			synchronized (fileLoungeList) {
				fileLounge = fileLoungeList.get(storageId);
				if(fileLounge == null) {
					fileLounge = fileLoungeFactory.createFileLounge(storageId);
					if(fileLounge == null) {
						return null;
					}
					
					fileLounge.setFileCloseListener(fileCloser);
					fileLoungeList.put(storageId, fileLounge);
					fileLoungeCleaners.add(timedExecutor.scheduleAtFixedRate(new FileLoungeCleaner(fileLounge), 0, cleanFrequencyMillis, TimeUnit.MILLISECONDS));
				}
			}
		}
		
		return fileLounge;
	}
	
	public void write(int storageId, DataItem[] items, DataHandleCallback<DataWriteResult> callback) {
		FileLounge fileLounge = getFileLoungeByStorageId(storageId);
		if(fileLounge == null) {
			callback.error(new StorageNameNonexistentException(storageId));
			return;
		}
		
		Arrays.sort(items, new Comparator<DataItem>() {

			@Override
			public int compare(DataItem o1, DataItem o2) {
				return o2.getBytes().length - o1.getBytes().length;
			}
			
		});
		
		LOG.debug("---size=={}", items.length);
		int[] sizes = new int[items.length];
		for(int i = 0; i < items.length; i++) {
			sizes[i] = items[i].getBytes().length;
		}
		
		FileLimiter[] fileList = fileLounge.getFileLimiterList(sizes);
		AsyncTaskGroup<ResultItem[]> taskGroup = new AsyncTaskGroup<ResultItem[]>();
		FileWriteCallback taskCallback = new FileWriteCallback(callback);
		for(int i = 0; i < fileList.length; i++) {
			FileLimiter file = fileList[i];
			if(file == null) {
				ResultItem item = new ResultItem();
				item.setSequence(items[i].getUserSequence());
				item.setFid(null);
				taskCallback.addResultItem(null);
				continue;
			}
			
			MultiDataWriteTask task = (MultiDataWriteTask) file.attach();
			if(task == null) {
				task = new MultiDataWriteTask(file, idManager, connectionPool, writeTaskExecutor, resultExecutor);
				file.attach(task);
				taskGroup.addTask(task);
			}
			
			task.addDataItem(items[i]);
		}
		
		multiTaskExecutor.submit(taskGroup, new FileWriteCallback(callback));
	}
	
	private class FileWriteCallback implements AsyncTaskGroupCallback<ResultItem[]> {
		private List<ResultItem> resultList = new ArrayList<ResultItem>();
		private DataHandleCallback<DataWriteResult> callback;
		
		public FileWriteCallback(DataHandleCallback<DataWriteResult> callback) {
			this.callback = callback;
		}
		
		public void addResultItem(ResultItem item) {
			resultList.add(item);
		}

		@Override
		public void completed(AsyncTaskResult<ResultItem[]>[] results) {
			//每个taskResult代表一个文件的数据写入结果
			for(AsyncTaskResult<ResultItem[]> taskResult : results) {
				if(taskResult.getError() != null) {
					//有异常的返回结果不处理
					continue;
				}
				
				for(ResultItem item : taskResult.getResult()) {
					//把数据汇总到统一的集合中
					resultList.add(item);
				}
			}
			
			ResultItem[] allResults = new ResultItem[resultList.size()];
			resultList.toArray(allResults);
			
			DataWriteResult dataWriteResult = new DataWriteResult();
			dataWriteResult.setItems(allResults);
			callback.completed(dataWriteResult);
		}
		
	}
	
	private class FileInvalidator implements FileNodeInvalidListener {

		@Override
		public void invalid() {
			LOG.warn("File Lounge is going to be cleaned!!");
			fileLoungeList.clear();
			for(ScheduledFuture<?> f : fileLoungeCleaners) {
				f.cancel(false);
			}
		}
		
	}
	
	private class FileLoungeHandler implements StorageNameStateListener {

		@Override
		public void storageNameAdded(StorageNameNode node) {
		}

		@Override
		public void storageNameUpdated(StorageNameNode node) {
		}

		@Override
		public void storageNameRemoved(StorageNameNode node) {
			FileLounge fileLounge = fileLoungeList.remove(node.getId());
			if(fileLounge != null) {
				for(FileLimiter fileLimiter : fileLounge.listFileLimiters()) {
					FileNode fileNode = fileLimiter.getFileNode();
					try {
						fileCloser.closeFileNode(fileNode);
					} catch (Exception e) {
						LOG.warn("clean to close file[{}] error", fileNode.getName());
					}
				}
				
				fileLounge.clean();
			}
		}
		
	}
	
	private class DefaultFileNodeSink implements FileNodeSink {

		@Override
		public Service getService() {
			return service;
		}

		@Override
		public void fill(FileNode fileNode) {
			FileLounge fileLounge = getFileLoungeByStorageId(fileNode.getStorageId());

			LOG.info("received transferred file--[{}]", fileNode.getName());
			fileRecovery.synchronize(fileNode, new FileSynchronizeCallback() {
				
				@Override
				public void complete(FileNode fileNode) {
					//同步不同副本之间的文件内容，然后获取正确的文件大小和文件序列号
					LOG.info("start rebuild file[{}]", fileNode.getName());
					
					int[] metaInfo = null;
					DuplicateNode[] nodes = fileNode.getDuplicateNodes();
					for(DuplicateNode node : nodes) {
						DiskNodeConnection connection = connectionPool.getConnection(node);
						
						LOG.info("connection ==" + connection);
						if(connection == null || connection.getClient() == null) {
							continue;
						}
						
						String serverId = idManager.getOtherSecondID(node.getId(), fileNode.getStorageId());
						String filePath = FilePathBuilder.buildFilePath(fileNode.getStorageName(), serverId, fileNode.getCreateTime(), fileNode.getName());
						metaInfo = connection.getClient().getWritingFileMetaInfo(filePath);
						
						if(metaInfo != null) {
							break;
						}
					}
					
					if(metaInfo == null) {
						LOG.error("Can not get Metadata of file[{}]", fileNode.getName());
						return;
					}
					
					LOG.info("rebuild file[{}] with length[{}], sequence[{}]", fileNode.getName(), metaInfo[1], metaInfo[0] + 1);
					FileLimiter file = new FileLimiter(fileNode, DuplicationEnvironment.DEFAULT_MAX_FILE_SIZE, metaInfo[1]/*文件大小*/, metaInfo[0] + 1/*文件序列号*/);
					fileLounge.addFileLimiter(file);
				}

				@Override
				public void error(Throwable cause) {
					LOG.error("reopen file[{}] failed!", fileNode.getName(), cause);
					try {
						//对于没办法处理的文件，只能删除节点，不再重用
						fileCoordinator.delete(fileNode);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
		
	}
}
