package com.bonree.brfs.duplication.datastream.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncTask;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.file.FileLimiter;
import com.bonree.brfs.duplication.datastream.handler.DataItem;

public class DataWriteTask extends AsyncTask<WriteTaskResult> {
	private static final Logger LOG = LoggerFactory.getLogger(DataWriteTask.class);
	
	private DiskNodeConnection connection;
	private FileLimiter file;
	private DataItem item;
	
	private String taskId;
	private String serverId;
	
	public DataWriteTask(String taskId, DiskNodeConnection connection, FileLimiter file, DataItem item, String serverId) {
		this.taskId = taskId;
		this.connection = connection;
		this.file = file;
		this.item = item;
		this.serverId = serverId;
	}

	@Override
	public String getTaskId() {
		return taskId;
	}

	@Override
	public WriteTaskResult run() throws Exception {
		long start = System.currentTimeMillis();
		try {
			if(connection == null) {
				return null;
			}
			
			DiskNodeClient client = connection.getClient();
			if(client == null) {
				return null;
			}
			
			LOG.info("write data to {}:{}", connection.getService().getHost(), connection.getService().getPort());
			
			String filePath = FilePathBuilder.buildPath(file.getFileNode(), serverId);
			
			LOG.info("writing {}[seq[{}]###size[{}]]", filePath, file.sequence(), item.getBytes().length);
			WriteResult result = client.writeData(filePath, file.sequence(), item.getBytes());
			WriteTaskResult taskResult = new WriteTaskResult();
			LOG.info("Write Result--{}", result);
			
			taskResult.setOffset(result.getOffset());
			taskResult.setSize(result.getSize());
			return taskResult;
		} finally {
			System.out.println("take##############" + (System.currentTimeMillis() - start));
		}
	}
	
}
