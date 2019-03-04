package com.bonree.brfs.schedulers.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
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
	public static final String CURRENT_INDEX = "CURRENT_INDEX";
	public static final String TASK_RESULT = "TASK_RESULT";
	public static final String BATCH_SIZE = "BATCH_SIZE";
	public static final String BASE_ROUTE_PATH = "BASE_ROUTE_PATH";
	public static final String CHECK_TTL = "CHECK_TTL";
	public static final String CURRENT_TASK_NAME = "CURRENT_TASK_NAME";
	public static final String CHECK_TIME_RANGE = "CHECK_TIME_RANGE";
	public static final String BASE_SERVER_ID_PATH = "BASE_SERVER_ID_PATH";
	
	
	/**
	 * 概述：生成采集job需要的参数
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createGatherResourceDataMap(ResourceTaskConfig resource, String serverId, String zkServerPath,String zkaddress){
		Map<String, String>  dataMap = new HashMap<>();
		dataMap.put(DATA_PATH, Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT));
		String host = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_HOST);
		dataMap.put(IP, host);
		dataMap.put(GATHER_INVERAL_TIME, resource.getGatherResourceInveralTime() + "");
		dataMap.put(CALC_RESOURCE_COUNT, resource.getCalcResourceValueCount() + "");
		dataMap.put(BASE_SERVER_ID_PATH, zkServerPath);
		dataMap.put(ZOOKEEPER_ADDRESS, zkaddress);
		return dataMap;
	}
	/**
	 * 概述：
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createAsynResourceDataMap(ResourceTaskConfig resource){
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
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,String> createCreateDataMap(ResourceTaskConfig resource){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(CHECK_TTL, resource.getCheckTtl()+"");
		return dataMap;
	}
	public static Map<String, String> createOperationDataMap(String taskName,String serviceId, TaskModel task, TaskRunPattern pattern,String path) throws Exception{
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(DATA_PATH, path);
		dataMap.put(TASK_NAME, taskName);
		dataMap.put(SERVER_ID, serviceId);
		dataMap.put(TASK_TYPE, task.getTaskType() +"");
		dataMap.put(TASK_STAT, task.getTaskState() + "");
		dataMap.put(TASK_OPERATION_ARRAYS, JsonUtils.toJsonStringQuietly(task.getAtomList()));
		List<AtomTaskModel> atoms = task.getAtomList();
		Map<Integer,BatchAtomModel> batchMap = groupAtoms(atoms, pattern.getRepeateCount());
		for(Map.Entry<Integer, BatchAtomModel> entry : batchMap.entrySet()) {
			dataMap.put(entry.getKey().toString(), JsonUtils.toJsonStringQuietly(entry.getValue()));
		}
		dataMap.put(TASK_REPEAT_RUN_COUNT, pattern.getRepeateCount() + "");
		dataMap.put(TASK_RUN_INVERAL_TIME, pattern.getSleepTime() + "");
		return dataMap;
	}
	public static Map<Integer,BatchAtomModel> groupAtoms(final List<AtomTaskModel> atoms, int batchCount) throws IllegalArgumentException{
		Map<Integer,BatchAtomModel> batchMap = new HashMap<Integer,BatchAtomModel>();
		BatchAtomModel batch = null;
		int size = atoms == null ? 0 : atoms.size();
		if(batchCount <= 0) {
			throw new IllegalArgumentException("batch time below zero !!!") ;
		}
		int batchSize = size % batchCount == 0 ? size/batchCount : size/batchCount + size%batchCount;
		int fromIndex = 0;
		int rSize = 0;
		for(int i = 1; i<=batchCount; i++) {
			if(!batchMap.containsKey(i)) {
				batchMap.put(i, new BatchAtomModel());
			}
			if(size == 0) {
				continue;
			}
			batch = batchMap.get(i);
			if(fromIndex > size) {
				continue;
			}else if(fromIndex+batchSize <=size) {
				batch.addAll(atoms.subList(fromIndex, fromIndex +batchSize));
			}else {
				batch.addAll(atoms.subList(fromIndex, size));
			}
			rSize += batch.getAtoms().size();
			fromIndex +=batchSize;
		}
		if(size !=rSize) {
			throw new IllegalArgumentException("total batch ["+rSize+"] not equal source ["+size+"], batch count ["+batchCount+"]") ;
		}
		return batchMap;
	}
	/**
	 * 概述：重启时，检查
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
	public static Map<String,String> createCopyCheckMap(ResourceTaskConfig config){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(CHECK_TTL, config.getCheckTtl()+"");
		return dataMap;
	}
	public static Map<String,String> createWatchJobMap(String zkHost){
		Map<String,String> dataMap = new HashMap<>();
		dataMap.put(ZOOKEEPER_ADDRESS, zkHost);
		return dataMap;
	}
	public static Map<String, String> createCOPYDataMap(String taskName,String serviceId, long invertalTime, String zkHost, String path,String dataPath){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(TASK_NAME, "");
		dataMap.put(SERVER_ID, serviceId);
		dataMap.put(TASK_TYPE, TaskType.SYSTEM_COPY_CHECK.code() + "");
		dataMap.put(TASK_STAT, TaskState.INIT + "");
		dataMap.put(ZOOKEEPER_ADDRESS, zkHost);
		dataMap.put(BASE_ROUTE_PATH, path);
		dataMap.put(TASK_REPEAT_RUN_COUNT, "-1");
		dataMap.put(TASK_RUN_INVERAL_TIME, invertalTime+"");
		dataMap.put(BATCH_SIZE, "10");
		dataMap.put(DATA_PATH, dataPath);
		
		return dataMap;
	}
	public static Map<String, String> createCylcCheckDataMap(int day) {
	    Map dataMap = new HashMap();
	    dataMap.put(CHECK_TIME_RANGE, day + "");
	    return dataMap;
	  }
	public static Map<String, String> createWatchDogDataMap(String zkHost, String path,String dataPath){
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(ZOOKEEPER_ADDRESS, zkHost);
		dataMap.put(BASE_ROUTE_PATH, path);
		dataMap.put(DATA_PATH, dataPath);
		return dataMap;
	}
}
