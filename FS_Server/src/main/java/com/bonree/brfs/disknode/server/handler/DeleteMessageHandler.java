package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.server.handler.data.DeleteData;

public class DeleteMessageHandler implements MessageHandler {
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	
	public DeleteMessageHandler(DiskContext context, FileWriterManager nodeManager) {
		this.diskContext = context;
		this.writerManager = nodeManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		try {
			String filePath = diskContext.getConcreteFilePath(msg.getPath());
			
			Map<String, String> params = msg.getParams();
			DeleteData data = new DeleteData();
			data.setForceClose(Boolean.parseBoolean(params.getOrDefault("force", "false")));
			data.setForceClose(Boolean.parseBoolean(params.getOrDefault("recursive", "false")));
			
			File targetFile = new File(filePath);
			if(targetFile.isFile()) {
				try {
					closeFile(targetFile, data.isForceClose());
					
					result.setSuccess(true);
				} catch (Exception e) {
					result.setSuccess(false);
					result.setCause(e);
					return;
				}
			} else {
				try {
					closeDir(targetFile, data.isRecursive(), data.isForceClose());
					result.setSuccess(true);
				} catch (Exception e) {
					result.setSuccess(false);
					result.setCause(e);
					return;
				}
			}
		} finally {
			callback.completed(result);
		}
	}
	
	private void closeFile(File file, boolean forceClose) throws IllegalStateException {
		if(writerManager.getBinding(file.getAbsolutePath(), false) == null) {
			file.delete();
			return;
		}
		
		if(!forceClose) {
			throw new IllegalStateException("File[" + file.getAbsolutePath() + "] is under writing now!");
		}
		
		writerManager.close(file.getAbsolutePath());
		file.delete();
	}
	
	private void closeDir(File dir, boolean recursive, boolean forceClose) {
		Queue<File> fileQueue = new LinkedList<File>();
		fileQueue.add(dir);
		
		File[] fileList = dir.listFiles();
		if(fileList.length == 0) {
			dir.delete();
			return;
		}
		
		while(!fileQueue.isEmpty()) {
			File file = fileQueue.poll();
			if(file.isDirectory()) {
				for(File child : file.listFiles()) {
					fileQueue.add(child);
				}
			} else {
				if(recursive) {
					throw new IllegalStateException("File exists in dir[" + dir.getAbsolutePath() + "]");
				}
				
				closeFile(file, forceClose);
			}
		}
	}

}
