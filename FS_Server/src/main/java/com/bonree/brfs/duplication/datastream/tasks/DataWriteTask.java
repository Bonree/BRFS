package com.bonree.brfs.duplication.datastream.tasks;

import com.bonree.brfs.common.asynctask.AsyncTask;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class DataWriteTask extends AsyncTask<DataWriteResult> {
	private DiskNodeConnection connection;
	private FileNode fileNode;
	private byte[] data;
	
	private String taskId;
	
	public DataWriteTask(String taskId, DiskNodeConnection connection, FileNode fileNode, byte[] data) {
		this.taskId = taskId;
		this.connection = connection;
		this.fileNode = fileNode;
		this.data = data;
	}

	@Override
	public String getTaskId() {
		return taskId;
	}

	@Override
	public DataWriteResult run() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
}
