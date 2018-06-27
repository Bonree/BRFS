package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;

public class ListMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ListMessageHandler.class);
	
	private DiskContext context;
	
	private LinkedList<FileInfo> fileList = new LinkedList<FileInfo>();
	
	public ListMessageHandler(DiskContext context) {
		this.context = context;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		String dirPath = null;
		try {
			dirPath = context.getConcreteFilePath(msg.getPath());
			int level = Integer.parseInt(msg.getParams().getOrDefault("level", "1"));
			
			File dir = new File(dirPath);
			if(!dir.exists()) {
				result.setSuccess(false);
				result.setCause(new FileNotFoundException(msg.getPath()));
				return;
			}
			
			if(!dir.isDirectory()) {
				result.setSuccess(false);
				result.setCause(new IllegalAccessException("[" + msg.getPath() + "] is not directory"));
				return;
			}
			
			FileInfo dirInfo = new FileInfo();
			dirInfo.setLevel(0);
			dirInfo.setType(FileInfo.TYPE_DIR);
			dirInfo.setPath(dirPath);
			fileList.addLast(dirInfo);
			
			ArrayList<FileInfo> fileInfoList = new ArrayList<FileInfo>();
			traverse(level, fileInfoList);
			result.setSuccess(true);
			result.setData(BrStringUtils.toUtf8Bytes(toJson(fileInfoList)));
		} catch (Exception e) {
			LOG.error("list dir[{}] error", dirPath, e);
			result.setSuccess(false);
		} finally {
			callback.completed(result);
		}
		
	}
	
	private void traverse(int level, ArrayList<FileInfo> fileInfoList) {
		while(!fileList.isEmpty()) {
			FileInfo fileInfo = fileList.remove();
			
			if(fileInfo.getType() == FileInfo.TYPE_DIR && fileInfo.getLevel() < level) {
				File[] subFiles = new File(fileInfo.getPath()).listFiles();
				if(subFiles != null && subFiles.length > 0) {
					for(File subFile : subFiles) {
						FileInfo info = new FileInfo();
						info.setLevel(fileInfo.getLevel() + 1);
						info.setType(subFile.isDirectory() ? FileInfo.TYPE_DIR : FileInfo.TYPE_FILE);
						info.setPath(subFile.getAbsolutePath());
						fileList.addLast(info);
					}
				}
			}
			
			fileInfo.setPath(context.getLogicFilePath(fileInfo.getPath()));
			fileInfoList.add(fileInfo);
		}
	}
	
	private String toJson(ArrayList<FileInfo> fileInfoList) {
		JSONArray array = new JSONArray();
		for(FileInfo file : fileInfoList) {
			JSONObject object = new JSONObject();
			object.put("type", file.getType());
			object.put("level", file.getLevel());
			object.put("path", file.getPath());
			array.add(object);
		}
		
		return array.toJSONString();
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
}
