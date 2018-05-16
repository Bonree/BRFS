package com.bonree.brfs.duplication.datastream.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.google.common.base.Splitter;

public class DeleteDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DeleteDataMessageHandler.class);
	
	private static final int TIME_INTERVAL_LEVEL = 2;
	
	private ServiceManager serviceManager;
	private StorageNameManager storageNameManager;
	
	public DeleteDataMessageHandler(ServiceManager serviceManager, StorageNameManager storageNameManager) {
		this.serviceManager = serviceManager;
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		int storageId = Integer.parseInt(msg.getPath().replaceAll("/", ""));
		
		LOG.info("DELETE data for storage[{}]", storageId);
		
		String path = getPathByStorageNameId(storageId);
		if(path == null) {
			result.setSuccess(false);
			result.setCause(new StorageNameNonexistentException(storageId));
			callback.completed(result);
			return;
		}
		
		Map<String, String> params = msg.getParams();
		if(!params.containsKey("start") || !params.containsKey("end")) {
			result.setSuccess(false);
			result.setCause(new Exception("start time and end time must be set!"));
			return;
		}
		
		long startTime = Long.parseLong(params.get("start"));
		long endTime = Long.parseLong(params.get("end"));
		LOG.info("DELETE DATA [{}-->{}]", startTime, endTime);
		
		
		List<Service> serviceList = serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		boolean deleteCompleted = true;
		for(Service service : serviceList) {
			DiskNodeClient client = null;
			try {
				client = new HttpDiskNodeClient(service.getHost(), service.getPort());
				List<FileInfo> fileList = client.listFiles(path, TIME_INTERVAL_LEVEL);
				LOG.info("get file list size={}", fileList.size());
				
				List<String> deleteList = filterByTime(fileList, startTime, endTime);
				if(deleteList.isEmpty()) {
					continue;
				}
				
				for(String deletePath : deleteList) {
					LOG.info("Deleting----[{}]", deletePath);
					deleteCompleted &= client.deleteDir(deletePath, true, true);
				}
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				CloseUtils.closeQuietly(client);
			}
		}
		
		result.setSuccess(deleteCompleted);
		callback.completed(result);
	}

	private String getPathByStorageNameId(int storageId) {
		StorageNameNode node = storageNameManager.findStorageName(storageId);
		if(node != null) {
			return "/" + node.getName();
		}
		
		return null;
	}
	
	private List<String> filterByTime(List<FileInfo> fileList, long startTime, long endTime) {
		ArrayList<String> fileNames = new ArrayList<String>();
		for(FileInfo info : fileList) {
			if(info.getLevel() != TIME_INTERVAL_LEVEL) {
				continue;
			}
			
			List<String> times = Splitter.on("_").splitToList(new File(info.getPath()).getName());
			if(startTime <= Long.parseLong(times.get(0)) && Long.parseLong(times.get(1)) <= endTime) {
				fileNames.add(info.getPath());
			}
		}
		
		return fileNames;
	}
}
