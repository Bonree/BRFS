package com.bonree.brfs.duplication.datastream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.shaded.com.google.common.primitives.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncExecutor;
import com.bonree.brfs.common.asynctask.AsyncTaskGroup;
import com.bonree.brfs.common.asynctask.AsyncTaskGroupCallback;
import com.bonree.brfs.common.asynctask.AsyncTaskResult;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileLimiter;
import com.bonree.brfs.duplication.datastream.file.FileLounge;
import com.bonree.brfs.duplication.datastream.tasks.DataWriteTask;
import com.bonree.brfs.duplication.datastream.tasks.WriteTaskResult;
import com.bonree.brfs.duplication.recovery.FileRecovery;
import com.bonree.brfs.duplication.recovery.FileRecoveryListener;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DuplicateWriter {
	private static final Logger LOG = LoggerFactory.getLogger(DuplicateWriter.class);
	
	private static final int DEFAULT_THREAD_NUM = 5;
	private AsyncExecutor executor = new AsyncExecutor(DEFAULT_THREAD_NUM);
	
	private DiskNodeConnectionPool connectionPool;
	private FileLounge fileLounge;
	
	private FileRecovery fileRecovery;
	private ServerIDManager idManager;
	
	public DuplicateWriter(FileLounge fileLounge, FileRecovery fileRecovery, ServerIDManager idManager, DiskNodeConnectionPool connectionPool) {
		this.fileLounge = fileLounge;
		this.fileRecovery = fileRecovery;
		this.idManager = idManager;
		this.connectionPool = connectionPool;
		
		this.fileLounge.setFileCloseListener(new FileNodeCloseListener());
	}
	
	public void write(int storageId, DataItem[] items, DataHandleCallback<DataWriteResult> callback) {
		EmitResultGather resultGather = new EmitResultGather(items.length, callback);
		LOG.debug("---size=={}", items.length);
		for(DataItem item : items) {
			if(item == null || item.getBytes() == null || item.getBytes().length == 0) {
				resultGather.putResultItem(new ResultItem(item.getSequence()));
				continue;
			}
			
			try {
				FileLimiter file = fileLounge.getFileLimiter(storageId, item.getBytes().length);
				LOG.debug("get FileLimiter[{}]", file);
				
				emitData(item, file, resultGather);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.info("####-->{}", e.toString());
				resultGather.putResultItem(new ResultItem(item.getSequence()));
			}
		}
	}
	
	private void emitData(DataItem item, FileLimiter file, EmitResultGather resultGather) {
		DiskNodeConnection[] connections = connectionPool.getConnections(file.getFileNode().getDuplicateNodes());
		LOG.debug("get Connections size={}", connections.length);
		
		AsyncTaskGroup<WriteTaskResult> taskGroup = new AsyncTaskGroup<WriteTaskResult>();
		for(int i = 0; i < connections.length; i++) {
			LOG.debug("get connection----{}", connections[i]);
			if(connections[i] != null) {
				String serverId = idManager.getOtherSecondID(file.getFileNode().getDuplicateNodes()[i].getId(), file.getFileNode().getStorageId());
				taskGroup.addTask(new DataWriteTask(connections[i].getService().getServiceId(), connections[i], file, item, serverId));
			}
		}
		
		executor.submit(taskGroup, new DataWriteResultCallback(item, file, resultGather));
	}
	
	private class DataWriteResultCallback implements AsyncTaskGroupCallback<WriteTaskResult> {
		private DataItem item;
		private FileLimiter file;
		private EmitResultGather resultGather;
		
		public DataWriteResultCallback(DataItem item, FileLimiter file, EmitResultGather resultGather) {
			this.item = item;
			this.file = file;
			this.resultGather = resultGather;
		}

		@Override
		public void completed(AsyncTaskResult<WriteTaskResult>[] results) {
			LOG.debug("Write result size----{}", results.length);
			List<WriteTaskResult> taskResultList = getValidResultList(results);
			
			if(taskResultList.isEmpty()) {
				LOG.error("None correct result is return from DiskNode! FILE[" + file.getFileNode().getName() + "]");
				
				file.release(item.getBytes().length);
				resultGather.putResultItem(new ResultItem(item.getSequence()));
				return;
			}
			
			for(WriteTaskResult result : taskResultList) {
				if(file.size() != (result.getOffset() + result.getSize())) {
					LOG.info("Write Task Result maybe ERROR!, expect[{}], but[{}]", file.size(), (result.getOffset() + result.getSize()));
					//TODO error: need to recover the file
				}
			}
			
			WriteTaskResult taskResult = taskResultList.get(0);
			
			ResultItem resultItem = new ResultItem(item.getSequence());
			resultItem.setFid(FidBuilder.getFid(file.getFileNode(), taskResult.getOffset(), taskResult.getSize()));
			resultGather.putResultItem(resultItem);
			file.release(0);
		}
		
		private List<WriteTaskResult> getValidResultList(AsyncTaskResult<WriteTaskResult>[] results) {
			List<WriteTaskResult> taskResultList = new ArrayList<WriteTaskResult>(results.length);
			for(AsyncTaskResult<WriteTaskResult> taskResult : results) {
				if(taskResult.getError() != null) {
					LOG.error("task[" + taskResult.getTaskId() + "] get error", taskResult.getError());
				}
				
				if(taskResult.getResult() != null) {
					taskResultList.add(taskResult.getResult());
				}
			}
			
			return taskResultList;
		}
	}
	
	private class EmitResultGather {
		private DataHandleCallback<DataWriteResult> callback;
		
		private AtomicInteger count = new AtomicInteger();
		private ResultItem[] resultItems;
		
		public EmitResultGather(int count, DataHandleCallback<DataWriteResult> callback) {
			this.resultItems = new ResultItem[count];
			this.callback = callback;
		}
		
		public void putResultItem(ResultItem item) {
			int index = count.getAndIncrement();
			resultItems[index] = item;
			
			if((index + 1) == resultItems.length) {
				DataWriteResult writeResult = new DataWriteResult();
				writeResult.setItems(resultItems);
				callback.completed(writeResult);
			}
		}
	}
	
	/**
	 * 文件关闭监听接口
	 * 
	 * 当收到文件关闭的通知后，需要通知DiskNode服务对其进行关闭
	 * 
	 * @author chen
	 *
	 */
	private class FileNodeCloseListener implements FileLounge.FileCloseListener {

		@Override
		public void close(FileLimiter file) {
			fileRecovery.recover(file.getFileNode(), new FileRecoveryListener() {
				
				@Override
				public void complete(FileNode file) {
					DiskNodeConnection[] connections = connectionPool.getConnections(file.getDuplicateNodes());
					for(int i = 0; i < connections.length; i++) {
						if(connections[i] != null) {
							DiskNodeClient client = connections[i].getClient();
							if(client != null) {
								String serverId = idManager.getOtherSecondID(file.getDuplicateNodes()[i].getId(), file.getStorageId());
								String filePath = FilePathBuilder.buildPath(file, serverId);
								
								try {
									WriteResult result = client.writeData(filePath, -2, Bytes.concat(FileEncoder.validate(0), FileEncoder.tail()));
									if(result != null) {
										client.closeFile(filePath);
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}

				@Override
				public void error(Throwable cause) {
					// TODO save the file node to handle it later
					cause.printStackTrace();
				}
				
			});
		}
		
	}
}
