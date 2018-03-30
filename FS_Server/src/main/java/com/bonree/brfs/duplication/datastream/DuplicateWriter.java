package com.bonree.brfs.duplication.datastream;

import com.bonree.brfs.common.asynctask.AsyncExecutor;
import com.bonree.brfs.common.asynctask.AsyncTaskGroup;
import com.bonree.brfs.disknode.client.WriteResult;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileInfo;
import com.bonree.brfs.duplication.datastream.file.FileLounge;
import com.bonree.brfs.duplication.datastream.handler.DataItem;
import com.bonree.brfs.duplication.datastream.tasks.DataWriteResult;
import com.bonree.brfs.duplication.datastream.tasks.DataWriteResultCallback;
import com.bonree.brfs.duplication.datastream.tasks.DataWriteTask;
import com.bonree.brfs.duplication.recovery.FileRecovery;

public class DuplicateWriter {
	
	private static final int DEFAULT_THREAD_NUM = 5;
	private AsyncExecutor executor = new AsyncExecutor(DEFAULT_THREAD_NUM);
	
	private DiskNodeConnectionPool connectionPool;
	private FileLounge fileLounge;
	private FileRecovery fileRecovery;
	
	public DuplicateWriter(FileLounge fileLounge, FileRecovery fileRecovery) {
		this.fileLounge = fileLounge;
		this.fileRecovery = fileRecovery;
	}
	
	public void write(int storageId, DataItem[] items, DataHandleCallback<DataWriteResult> callback) {
		for(DataItem item : items) {
			if(item == null || item.getBytes() == null) {
				continue;
			}
			
			byte[] bytes = item.getBytes();
			
			try {
				FileInfo file = fileLounge.getFileInfo(storageId, bytes.length);
				
				emitData(bytes, file.getFileNode(), new EmitCallback(file.getFileNode(), callback));
			} catch (Exception e) {
				callback.error(e);
			}
		}
	}
	
	public void emitData(byte[] data, FileNode fileNode, EmitCallback callback) {
		DuplicateNode[] duplicates = fileNode.getDuplicateNodes();
		
		DiskNodeConnection[] connections = new DiskNodeConnection[duplicates.length];
		for(int i = 0; i < connections.length; i++) {
			connections[i] = connectionPool.getConnection(duplicates[i]);
		}
		
		AsyncTaskGroup<DataWriteResult> taskGroup = new AsyncTaskGroup<DataWriteResult>();
		for(DiskNodeConnection connection : connections) {
			taskGroup.addTask(new DataWriteTask("***", connection, fileNode, data));
		}
		
		executor.submit(taskGroup, new DataWriteResultCallback());
	}
	
	private class EmitCallback {
		private FileNode fileNode;
		private DataHandleCallback<DataWriteResult> callback;
		
		public EmitCallback(FileNode fileNode, DataHandleCallback<DataWriteResult> callback) {
			this.fileNode = fileNode;
			this.callback = callback;
		}

		public void success(WriteResult result) {
			DataWriteResult writeResult = new DataWriteResult();
			//TODO 生成FID
			writeResult.setFid(null);
			callback.completed(writeResult);
		}

		public void recover() {
			fileLounge.deleteFile(fileNode);
			fileRecovery.recover(fileNode);
		}
		
	}
}
