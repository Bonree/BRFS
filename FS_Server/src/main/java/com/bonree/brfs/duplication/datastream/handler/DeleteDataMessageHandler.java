package com.bonree.brfs.duplication.datastream.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.configuration.units.DuplicateNodeConfigs;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.exception.StorageNameNonexistentException;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.google.common.base.Splitter;

public class DeleteDataMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DeleteDataMessageHandler.class);
	private static final long granule = TimeUnit.MINUTES.toMillis(Configs.getConfiguration()
		.GetConfig(DuplicateNodeConfigs.CONFIG_FILE_PATITION_INTERVAL_MINUTES));
	private static final int TIME_INTERVAL_LEVEL = 2;
	
	private ServiceManager serviceManager;
	private StorageNameManager storageNameManager;
	private ZookeeperPaths zkPaths;
	
	public DeleteDataMessageHandler(ZookeeperPaths zkPaths,ServiceManager serviceManager, StorageNameManager storageNameManager) {
		this.zkPaths = zkPaths;
	    this.serviceManager = serviceManager;
		this.storageNameManager = storageNameManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		
		List<String> deleteInfo = Splitter.on("/").omitEmptyStrings().trimResults().splitToList(msg.getPath());
		if(deleteInfo.size() != 2) {
			result.setSuccess(false);
			result.setCause(new IllegalArgumentException(msg.getPath()));
			callback.completed(result);
			return;
		}
		
		int storageId = Integer.parseInt(deleteInfo.get(0));
		
		LOG.info("DELETE data for storage[{}]", storageId);
		
//		String path = getPathByStorageNameId(storageId);
		StorageNameNode sn = storageNameManager.findStorageName(storageId);
		if(sn == null) {
			result.setSuccess(false);
			result.setCause(new StorageNameNonexistentException(storageId));
			callback.completed(result);
			LOG.info("storage[{}] is null", storageId);
			return;
		}
		
		List<String> times = Splitter.on("_").omitEmptyStrings().trimResults().splitToList(deleteInfo.get(1));
		ReturnCode code = checkTime(times.get(0), times.get(1), sn.getCreateTime(), granule);
		if(!ReturnCode.SUCCESS.equals(code)) {
			result.setSuccess(false);
			result.setData(BrStringUtils.toUtf8Bytes(code.name()));
			callback.completed(result);
			LOG.info("DELETE DATE Fail storage[{}] reason : {}", storageId, code.name());
			return;
		}
		long startTime = DateTime.parse(times.get(0)).getMillis();
		long endTime = DateTime.parse(times.get(1)).getMillis();
		LOG.info("DELETE DATA [{}-->{}]", times.get(0), times.get(1));
		
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME));

        code = TasksUtils.createUserDeleteTask(serviceList, zkPaths, sn, startTime, endTime,granule);
        
		result.setSuccess(ReturnCode.SUCCESS.equals(code));
		result.setData(BrStringUtils.toUtf8Bytes(code.name()));
		callback.completed(result);
	}
	/***
	 * 检查时间
	 */
	private ReturnCode checkTime(String startTimeStr, String endTimeStr, long cTime, long granule) {
		long startTime = 0L;
		long endTime = 0L;
		try {
			startTime = DateTime.parse(startTimeStr).getMillis();
			endTime = DateTime.parse(endTimeStr).getMillis();
		} catch (Exception e) {
			LOG.warn("starttime and endTime formate error !!! startTime: {} ,endTime: {}",startTimeStr,endTimeStr);
			return ReturnCode.TIME_FORMATE_ERROR;
		}
		// 1，时间格式不对
		if(startTime != (startTime - startTime%granule)
				|| endTime !=(endTime - endTime%granule)) {
			return ReturnCode.TIME_GRANULE_ERROR;
		}
		long currentTime = System.currentTimeMillis();
		long cuGra = currentTime - currentTime%granule;
		long sGra = startTime - startTime%granule;
		long eGra = endTime - endTime%granule;
		long cGra = cTime - cTime%granule;
		// 2.开始时间等于结束世界
		if(sGra >= eGra) {
			return ReturnCode.PARAMETER_ERROR;
		}
		// 3.结束时间小于创建时间
		if(cTime > eGra) {
			return ReturnCode.TIME_EARLIER_THAN_CREATE_ERROR;
		}
		// 4.当前时间
		if(cuGra <= sGra || cuGra<eGra) {
			return ReturnCode.FORBID_DELETE_CURRENT_ERROR;
		}
		// 若成功则返回null
		return ReturnCode.SUCCESS;
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
			if(startTime <= DateTime.parse(times.get(0)).getMillis() && DateTime.parse(times.get(1)).getMillis() <= endTime) {
				fileNames.add(info.getPath());
			}
		}
		
		return fileNames;
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return message.getPath().matches("/.*/.*_.*");
	}
}
