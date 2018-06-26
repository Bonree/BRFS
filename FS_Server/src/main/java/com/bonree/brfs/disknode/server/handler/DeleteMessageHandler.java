package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.server.handler.data.DeleteData;

public class DeleteMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DeleteMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private ExecutorService threadPool;
	
	public DeleteMessageHandler(DiskContext context, FileWriterManager nodeManager, ExecutorService threadPool) {
		this.diskContext = context;
		this.writerManager = nodeManager;
		this.threadPool = threadPool;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				HandleResult result = new HandleResult();
				
				try {
					String filePath = diskContext.getConcreteFilePath(msg.getPath());
					
					Map<String, String> params = msg.getParams();
					LOG.info("delete params--{}", params);
					
					DeleteData data = new DeleteData();
					data.setForceClose(params.containsKey("force") ? true : false);
					data.setRecursive(params.containsKey("recursive") ? true : false);
					
					LOG.info("deleting path[{}], force[{}], recursive[{}]", filePath, data.isForceClose(), data.isRecursive());
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
				} catch(Exception e) {
					LOG.error("delete message error", e);
					result.setSuccess(false);
				} finally {
					callback.completed(result);
				}
			}
		});
	}
	
	private void closeFile(File file, boolean forceClose) throws IllegalStateException {
		LOG.info("DISK Deleting file[{}]", file.getAbsolutePath());
		
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
		LinkedList<File> deletingDirs = new LinkedList<File>();
		fileQueue.add(dir);
		
		
		if(dir.list().length == 0) {
			dir.delete();
			return;
		}
		
		if(!recursive) {
			throw new IllegalStateException("Directory[" + dir.getAbsolutePath() + "] is not empty!");
		}
		
		//第一轮先删除普通文件节点
		while(!fileQueue.isEmpty()) {
			File file = fileQueue.poll();
			if(file.isDirectory()) {
				for(File child : file.listFiles()) {
					fileQueue.add(child);
				}
				
				deletingDirs.addFirst(file);
			} else {
				closeFile(file, forceClose);
			}
		}
		
		//第二轮删除文件夹节点
		for(File deleteDir : deletingDirs) {
			LOG.info("DISK Deleting dir[{}]", deleteDir.getAbsolutePath());
			deleteDir.delete();
		}
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return !message.getPath().isEmpty();
	}

}
