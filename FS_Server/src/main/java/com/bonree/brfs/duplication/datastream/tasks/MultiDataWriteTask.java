package com.bonree.brfs.duplication.datastream.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncExecutor;
import com.bonree.brfs.common.asynctask.AsyncTask;
import com.bonree.brfs.common.asynctask.AsyncTaskGroup;
import com.bonree.brfs.common.asynctask.AsyncTaskGroupCallback;
import com.bonree.brfs.common.asynctask.AsyncTaskResult;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.ResultItem;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileLimiter;
import com.bonree.brfs.server.identification.ServerIDManager;

public class MultiDataWriteTask extends AsyncTask<ResultItem[]> {
	private static Logger LOG = LoggerFactory.getLogger(MultiDataWriteTask.class);
	
	private FileLimiter file;
	private List<DataItem> dataList = new ArrayList<DataItem>();
	
	private DiskNodeConnectionPool connectionPool;
	private AsyncExecutor taskRunner;
	private ExecutorService resultHandleExecutor;
	private ServerIDManager idManager;
	
	private ArrayBlockingQueue<ResultItem[]> resultGetter = new ArrayBlockingQueue<ResultItem[]>(1);
	
	public MultiDataWriteTask(FileLimiter file,
			ServerIDManager idManager,
			DiskNodeConnectionPool connectionPool,
			AsyncExecutor executor,
			ExecutorService resultExecutor) {
		this.file = file;
		this.idManager = idManager;
		this.connectionPool = connectionPool;
		this.taskRunner = executor;
		this.resultHandleExecutor = resultExecutor;
	}
	
	public FileLimiter getFileLimiter() {
		return file;
	}
	
	public void addDataItem(DataItem item) {
		this.dataList.add(item);
	}
	
	public List<DataItem> getDataItemList() {
		return dataList;
	}

	@Override
	public ResultItem[] run() throws Exception {
		WriteData[] datas = new WriteData[dataList.size()];
		int sequence = file.getSequence();
		for(int i = 0; i < datas.length; i++) {
			datas[i] = new WriteData();
			datas[i].setDiskSequence(sequence++);
			datas[i].setBytes(dataList.get(i).getBytes());
		}
		
		AsyncTaskGroup<WriteResult[]> taskGroup = new AsyncTaskGroup<WriteResult[]>();
		for(DuplicateNode node : file.getFileNode().getDuplicateNodes()) {
			String serverID = idManager.getOtherSecondID(node.getId(), file.getFileNode().getStorageId());
			DiskNodeConnection connection = connectionPool.getConnection(node);
			String filePath = FilePathBuilder.buildPath(file.getFileNode(), serverID);
			taskGroup.addTask(new DataWriteTask(filePath, connection, datas));
		}
		
		DataWriteResultCallback callback = new DataWriteResultCallback(file);
		taskRunner.submit(taskGroup, callback, resultHandleExecutor);
		
		return resultGetter.take();
	}
	
	private class DataWriteResultCallback implements AsyncTaskGroupCallback<WriteResult[]> {
		private FileLimiter file;
		
		public DataWriteResultCallback(FileLimiter file) {
			this.file = file;
		}

		@Override
		public void completed(AsyncTaskResult<WriteResult[]>[] results) {
			LOG.debug("handle Writing result for file[{}]", file.getFileNode().getName());
			//先释放文件的锁定状态
			
			ResultItem[] resultItems = new ResultItem[dataList.size()];
			try {
				for(int i = 0; i < resultItems.length; i++) {
					//初始化返回结果列表，只设置数据序列号
					resultItems[i] = new ResultItem();
					resultItems[i].setSequence(dataList.get(i).getUserSequence());
				}
				
				int validIndex = -1;
				for(AsyncTaskResult<WriteResult[]> taskResult : results) {
					//每个taskResult对象代表一个磁盘节点的数据写入结果
					if(taskResult.getError() != null) {
						//如果有异常，说明某个磁盘节点写入数据失败
						LOG.warn("writing task is failed--{}", taskResult.getError());
						continue;
					}
					
					//磁盘节点写入结果正常返回
					WriteResult[] writeResults = taskResult.getResult();
					if(writeResults != null) {
						for(int i = 0; i < writeResults.length; i++) {
							//遍历每条数据的返回结果，如果结果中的size大于0，说明写入成功
							WriteResult writeResult = writeResults[i];
							if(writeResult != null && writeResult.getSize() > 0) {
								//写入成功的数据都是连续的（磁盘节点保证这一点），所以
								//只要记录最大的有效索引即可
								validIndex = Math.max(validIndex, i);
							}
						}
					}
				}
				
				LOG.debug("result valid index = {}", validIndex);
				//有效索引范围内的数据才能生存FID
				for(int i = 0; i < validIndex + 1; i++) {
					//文件所在的文件偏移量和数据大小都是通过FileLimiter中的信息计算的
					long offset = file.getLength();
					int size = dataList.get(i).getBytes().length;
					
					//更新文件的逻辑长度信息
					file.setLength(offset + size);
					
					String fid = FidBuilder.getFid(file.getFileNode(), offset, size);
					LOG.debug("get FID-->{}", fid);
					resultItems[i].setFid(fid);
				}
				
				file.incrementSequenceBy(validIndex + 1);
			} finally {
				file.attach(null);
				file.unlock();
			}
			
			try {
				resultGetter.put(resultItems);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
