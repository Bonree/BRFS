package com.bonree.brfs.disknode.server.tcp.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.HandleCallback;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.tcp.handler.data.ListFileMessage;

public class ListFileMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ListFileMessageHandler.class);
	
	private DiskContext context;
	
	private LinkedList<FileInfo> fileList = new LinkedList<FileInfo>();
	
	public ListFileMessageHandler(DiskContext context) {
		this.context = context;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, HandleCallback callback) {
		ListFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), ListFileMessage.class);
		if(message == null) {
			callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		String dirPath = null;
		try {
			dirPath = context.getConcreteFilePath(message.getPath());
			
			File dir = new File(dirPath);
			if(!dir.exists()) {
				callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
				return;
			}
			
			if(!dir.isDirectory()) {
				callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
				return;
			}
			
			FileInfo dirInfo = new FileInfo();
			dirInfo.setLevel(0);
			dirInfo.setType(FileInfo.TYPE_DIR);
			dirInfo.setPath(dirPath);
			fileList.addLast(dirInfo);
			
			ArrayList<FileInfo> fileInfoList = new ArrayList<FileInfo>();
			traverse(message.getLevel(), fileInfoList);
			
			BaseResponse response = new BaseResponse(baseMessage.getToken(), ResponseCode.OK);
			response.setBody(JsonUtils.toJsonBytes(fileInfoList));
			callback.complete(response);
		} catch (Exception e) {
			LOG.error("list dir[{}] error", dirPath, e);
			callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
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
}
