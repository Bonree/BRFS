package com.bonree.brfs.disknode.server.tcp.handler;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.ResponseWriter;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.tcp.handler.data.ListFileMessage;

public class ListFileMessageHandler implements MessageHandler<BaseResponse> {
	private static final Logger LOG = LoggerFactory.getLogger(ListFileMessageHandler.class);
	//TODO : 二期优化副本数校验任务，希望只返回与brfs文件系统有关的文件及目录
	private DiskContext context;
	
	private LinkedList<FileInfo> fileList = new LinkedList<FileInfo>();
	
	public ListFileMessageHandler(DiskContext context) {
		this.context = context;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, ResponseWriter<BaseResponse> writer) {
		ListFileMessage message = ProtoStuffUtils.deserialize(baseMessage.getBody(), ListFileMessage.class);
		if(message == null) {
			writer.write(new BaseResponse(ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		String dirPath = null;
		try {
			dirPath = context.getConcreteFilePath(message.getPath());
			Path d = Paths.get(dirPath);
			if(d.getFileName().toString().startsWith("0_")) {
			    d = d.getParent();
			}
			
			File dir = d.toFile();
			if(!dir.exists()) {
				writer.write(new BaseResponse(ResponseCode.ERROR));
				return;
			}
			
			if(!dir.isDirectory()) {
				writer.write(new BaseResponse(ResponseCode.ERROR));
				return;
			}
			
			FileInfo dirInfo = new FileInfo();
			dirInfo.setLevel(0);
			dirInfo.setType(FileInfo.TYPE_DIR);
			dirInfo.setPath(dirPath);
			fileList.addLast(dirInfo);
			
			ArrayList<FileInfo> fileInfoList = new ArrayList<FileInfo>();
			traverse(message.getLevel(), fileInfoList);
			
			BaseResponse response = new BaseResponse(ResponseCode.OK);
			response.setBody(JsonUtils.toJsonBytes(fileInfoList));
			writer.write(response);
		} catch (Exception e) {
			LOG.error("list dir[{}] error", dirPath, e);
			writer.write(new BaseResponse(ResponseCode.ERROR));
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
