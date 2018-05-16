package com.bonree.brfs.schedulers.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;

public class JobDataMapConstract {
	/**
	 * zookeeper地址
	 */
	public static final String ZOOKEEPER_ADDRESS = "ZOOKEEPER_ADDRESS";
	/**
	 * 任务过期时间
	 */
	public static final String TASK_EXPIRED_TIME = "TASK_EXPIRED_TIME";
	/**
	 * serverid
	 */
	public static final String SERVER_ID = "SERVER_ID";
	/**
	 * 数据目录
	 */
	public static final String DATA_PATH = "DATA_PATH";
	/**
	 * 集群分组
	 */
	public static final String CLUSTER_NAME = "CLUSTER_NAME";
	/**
	 * ip地址
	 */
	public static final String IP = "IP";
	/**
	 * 采集样本的间隔
	 */
	public static final String GATHER_INVERAL_TIME = "GATHER_INVERAL_TIME";
	/**
	 * 当样本数为几个是计算
	 */
	public static final String CALC_RESOURCE_COUNT = "CALC_RESOURCE_COUNT";
	/**
	 * 可用server的实现类
	 */
	public static final String AVAIABLE_SERVER_CLASS = "AVAIABLE_SERVER_CLASS";
	/**
	 * 任务重复次数
	 */
	public static final String TASK_REPEAT_RUN_COUNT = "REPEAT_RUN_COUNT";
	/**
	 * 任务执行间隔
	 */
	public static final String TASK_RUN_INVERAL_TIME = "TASK_RUN_INVERAL_TIME";
	/**
	 * 任务操作的队列
	 */
	public static final String TASK_OPERATION_ARRAYS = "TASK_OPERATION_ARRAYS";
	public static final String TASK_NAME = "TASK_NAME";
	public static final String TASK_TYPE = "TASK_TYPE";
	public static final String TASK_STAT = "TASK_STAT";
	public static final String TASK_MAP_STAT = "TASK_MAP_STAT";
	public static final String CURRENT_INDEX = "CURRENT_INDEX";
	public static final String TASK_RESULT = "TASK_RESULT";
	public static final String BATCH_SIZE = "BATCH_SIZE";
	public static final String CURRENT_TASK_NAME = "CURRENT_TASK_NAME";
	
	/**
	 * 概述：生成采集job需要的参数
	 * @param server
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	//TODO: 存在临时参数，需要确认serverId在启动时哪里获取到
	public static Map<String,String> createGatherResourceDataMap(ServerConfig server, ResourceTaskConfig resource, String serverId){
		Map<String, String>  dataMap = new HashMap<>();
		//TODO: 存在临时参数，需要确认serverId在启动时哪里获取到
		dataMap.put(DATA_PATH, server.getDataPath());
		dataMap.put(IP, server.getHost());
		dataMap.put(GATHER_INVERAL_TIME, resource.getGatherResourceInveralTime() + "");
		dataMap.put(CALC_RESOURCE_COUNT, resource.getCalcResourceValueCount() + "");
		return dataMap;
	}
	/**
	 * 概述：
	 * @param server
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createAsynResourceDataMap(ServerConfig server, ResourceTaskConfig resource){
		Map<String, String>  dataMap = new HashMap<>();
		dataMap.put(GATHER_INVERAL_TIME, resource.getGatherResourceInveralTime() + "");
		dataMap.put(CALC_RESOURCE_COUNT, resource.getCalcResourceValueCount() + "");
		dataMap.put(AVAIABLE_SERVER_CLASS, RandomAvailable.class.getCanonicalName());
		return dataMap;
	}
	/**
	 * 概述：任务管理信息
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createMetaDataMap(ResourceTaskConfig resource){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(TASK_EXPIRED_TIME, resource.getTaskExpiredTime() + "");
		return dataMap;
	}
	/**
	 * 概述：创建任务信息
	 * @param server
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createCreateDataMap(ServerConfig server, ResourceTaskConfig resource){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(DATA_PATH, server.getDataPath());
		return dataMap;
	}
	public static Map<String, String> createOperationDataMap(String taskName,String serviceId, TaskModel task, TaskRunPattern pattern){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(TASK_NAME, taskName);
		dataMap.put(SERVER_ID, serviceId);
		dataMap.put(TASK_TYPE, task.getTaskType() +"");
		dataMap.put(TASK_STAT, task.getTaskState() + "");
		dataMap.put(TASK_OPERATION_ARRAYS, JsonUtils.toJsonString(task.getAtomList()));
		List<AtomTaskModel> atoms = task.getAtomList();
		int size = atoms == null ? 0 : atoms.size();
		int count = size / pattern.getRepeateCount();
		BatchAtomModel batch = null;
		List<AtomTaskModel> tmp = null;
		int index = 0;
		for(int i = 1; i <= count; i+=count){
			batch = new BatchAtomModel();
			if(index + count <= size){
			tmp = atoms.subList(index, index + count);
			}else if(size > 0){
				tmp = atoms.subList(index, size - 1);
			}else{
				tmp = new ArrayList<AtomTaskModel>();
			}
			batch.addAll(tmp);
			dataMap.put(i +"", JsonUtils.toJsonString(batch));
			index = index +count;
		}
		dataMap.put(TASK_REPEAT_RUN_COUNT, pattern.getRepeateCount() + "");
		dataMap.put(TASK_RUN_INVERAL_TIME, pattern.getSleepTime() + "");
		return dataMap;
	}
	/**
	 * 概述：重启时，检查
	 * @param switchList
	 * @param release
	 * @param isReboot
	 * @param serverId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String, String> createRebootTaskOpertionDataMap(String dataPath,Map<String,String> switchMap) {
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(DATA_PATH, dataPath);
		if (switchMap != null && switchMap.isEmpty()) {
			dataMap.putAll(switchMap);
		}
		return dataMap;
	}
}
