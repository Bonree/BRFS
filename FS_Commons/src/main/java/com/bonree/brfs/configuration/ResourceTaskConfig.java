package com.bonree.brfs.configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午3:31:45
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务资源管理管理配置
 *****************************************************************************
 */
public class ResourceTaskConfig {
	/**
	 * 整体任务控制开关标识
	 */
	public final static String TASK_FRAMEWORK_SWITCH = "task.framework.switch";
	/**
	 * 任务过期时间
	 */
	public final static String TASK_EXPIRED_TIME = "task.expired.time";
	
	/**
	 * 任务服务创建任务检查的时间间隔单位ms
	 */
	public final static String CREATE_TASK_INTERVAL_TIME = "system.create.task.inverval.time";
	/**
	 * 执行任务管理定时提交执行任务的间隔 单位ms
	 */
	public final static String EXECUTE_TASK_INTERVAL_TIME = "execute.task.inverval.time";
	
	/**
	 * 系统删除任务开关标识
	 */
	public final static String SYSTEM_DELETE_SWITCH = "system.delete.pool.switch";
	/**
	 * 系统归并任务开关标识
	 */
	public final static String SYSTEM_MERGE_SWITCH = "system.merge.pool.switch";
	/**
	 * 系统校验任务开关标识
	 */
	public final static String SYSTEM_CHECK_SWITCH = "system.check.pool.switch";
	/**
	 * 系统副本校验任务开关标识
	 */
	public final static String SYSTEM_COPY_CHECK_SWITCH = "system.copy.pool.switch";
	/**
	 * 系统副本恢复任务开关标识
	 */
	public final static String SYSTEM_RECOVERY_SWITCH = "system.recovery.pool.switch";
	/**
	 * 用户删除任务开关标识
	 */
	public final static String USER_DELETE_SWITCH = "user.delete.pool.switch";
	/**
	 * 系统删除任务线程池大小标识
	 */
	public final static String SYSTEM_DELETE_POOL_SIZE = "system.delete.pool.size";
	/**
	 * 系统归并任务线程池大小标识
	 */
	public final static String SYSTEM_MERGE_POOL_SIZE = "system.merge.pool.size";
	/**
	 * 系统校验任务线程池大小标识
	 */
	public final static String SYSTEM_CHECK_POOL_SIZE = "system.check.pool.size";
	/**
	 * 系统副本恢复任务线程池大小标识
	 */
	public final static String SYSTEM_RECOVERY_POOL_SIZE = "system.recovery.pool.size";
	/**
	 * 用户删除任务线程池大小标识
	 */
	public final static String USER_DELETE_POOL_SIZE = "user.delete.pool.size";
	/**
	 * 系统副本校验任务开关标识
	 */
	public final static String SYSTEM_COPY_CHECK_POOL_SIZE = "system.copy.pool.size";
	
	/**
	 * 资源采集开关标识
	 */
	public final static String RESOURCE_FRAMEWORK_SWITCH = "resource.framework.switch";
	/**
	 * 资源采集依赖的第三包位置
	 */
	public final static String RESOURCE_LIB_PATH = "resource_lib_path";
	/**
	 * 资源采集点之间的时间间隔，单位ms
	 */
	public final static String GATHER_RESOURCE_INVERAL_TIME = "gathre.resource.inveral.time";
	/**
	 * 当采集了N个资源点后进行资源值计算
	 */
	public final static String CALC_RESOURCE_VALUE_COUNT = "calc.resource.value.count";
	
	public final static String SYSTEM_COPY_INTERVAL_TIME = "system.copy.check.create.inveratal.time";
	
	/**
	 * 资源限制配置
	 */
	public final static String LIMIT_RESOURCE_VALUE_CPU = "limit.resource.value.cpurate";
	public final static String LIMIT_RESOURCE_VALUE_MEMORY = "limit.resource.value.memoryrate";
	public final static String LIMIT_RESOURCE_VALUE_DISK_REMAIN = "limit.resource.value.disakremainrate";
	public final static String LIMIT_RESOURCE_VALUE_DISK_READ = "limit.resource.value.diskreadspeedrate";
	public final static String LIMIT_RESOURCE_VALUE_DISK_WRITE = "limit.resource.value.diskwritespeedrate";
	public final static String LIMIT_RESOURCE_VALUE_NET_TX = "limit.resource.value.nettspeedrate";
	public final static String LIMIT_RESOURCE_VALUE_NET_RX = "limit.resource.value.netrspeedrate";
	
	/**
	 * 任务开关
	 */
	Map<String, Boolean> taskPoolSwitchMap = new ConcurrentHashMap<String, Boolean>();
	/**
	 * 任务线程池大小
	 */
	Map<String, Integer> taskPoolSizeMap = new ConcurrentHashMap<String, Integer>();
	
	//创建任务执行的时间间隔 ms
	private long createTaskIntervalTime = 60000;
	private long executeTaskIntervalTime = 60000;
	private long gatherResourceInveralTime = 60000;
	private long taskExpiredTime = 7*24*60*60*1000;
	private int calcResourceValueCount = 2;
	private boolean taskFrameWorkSwitch = true;
	private boolean resourceFrameWorkSwitch = true;
	private String libPath;
	private double limitCpuRate = 0.9;
	private double limitMemoryRate = 0.9;
	private double limitDiskRemaintRate = 0.01;
	private double limitDiskReadRate = 0.9;
	private double limitDiskWriteRate = 0.9;
	private double limitNetTxRate = 0.9;
	private double limitNetRxRate = 0.9;
	private long createCheckJobTaskervalTime = 60000;
	
	
	private ResourceTaskConfig(){
		
	}
	public static ResourceTaskConfig parse(Configuration config) throws NullPointerException{
		ResourceTaskConfig conf = new ResourceTaskConfig();
		if(config == null){
			throw new NullPointerException("configuration is empty !!");
		}
		Map configMap = conf.getTaskPoolSwitchMap();
		String sysDelSwitch = config.getProperty(SYSTEM_DELETE_SWITCH, "true");
		String sysMergeSwitch = config.getProperty(SYSTEM_MERGE_SWITCH, "false");
		String sysCheckSwitch = config.getProperty(SYSTEM_CHECK_SWITCH, "false");
		String sysRecoverySwitch = config.getProperty(SYSTEM_RECOVERY_SWITCH, "false");
		String userDelSwitch = config.getProperty(USER_DELETE_SWITCH, "true");
		String sysCopySwitch = config.getProperty(SYSTEM_COPY_CHECK_SWITCH,"true");
		boolean sysDelFlag = Boolean.valueOf(sysDelSwitch);
		boolean sysMergeFlag = Boolean.valueOf(sysMergeSwitch);
		boolean sysCheckFlag =  Boolean.valueOf(sysCheckSwitch);
		boolean sysRecoveryFlag =  Boolean.valueOf(sysRecoverySwitch);
		boolean userDelFlag =  Boolean.valueOf(userDelSwitch);
		boolean sysCopyFlag = Boolean.valueOf(sysCopySwitch);
		configMap.put(TaskType.SYSTEM_DELETE.name(), sysDelFlag );
		configMap.put(TaskType.SYSTEM_MERGER.name(), sysMergeFlag );
		configMap.put(TaskType.SYSTEM_CHECK.name(), sysCheckFlag);
		configMap.put(TaskType.SYSTEM_RECOVERY.name(), sysRecoveryFlag);
		configMap.put(TaskType.USER_DELETE.name(), userDelFlag);
		configMap.put(TaskType.SYSTEM_COPY_CHECK.name(), sysCopyFlag);
		
		Map poolMap = conf.getTaskPoolSizeMap();

		String sysDelPoolSize = config.getProperty(SYSTEM_DELETE_POOL_SIZE, "1");
		String sysMergePoolSize = config.getProperty(SYSTEM_MERGE_POOL_SIZE, "1");
		String sysCheckPoolSize = config.getProperty(SYSTEM_CHECK_POOL_SIZE, "1");
		String sysRecoveryPoolSize = config.getProperty(SYSTEM_RECOVERY_POOL_SIZE, "1");
		String userDelPoolSize = config.getProperty(USER_DELETE_POOL_SIZE, "1");
		String sysCopyPoolSize = config.getProperty(SYSTEM_COPY_CHECK_POOL_SIZE, "1");

		int sysDelPool = Integer.valueOf(sysDelPoolSize); 
		int sysMergePool = Integer.valueOf(sysMergePoolSize); 
		int sysCheckPool = Integer.valueOf(sysCheckPoolSize);
		int sysRecoveryPool = Integer.valueOf(sysRecoveryPoolSize); 
		int userDelPool = Integer.valueOf(userDelPoolSize);
		int sysCopyPool = Integer.valueOf(sysCopyPoolSize);

		poolMap.put(TaskType.SYSTEM_DELETE.name(), sysDelPool );
		poolMap.put(TaskType.SYSTEM_MERGER.name(), sysMergePool );
		poolMap.put(TaskType.SYSTEM_CHECK.name(), sysCheckPool );
		poolMap.put(TaskType.SYSTEM_RECOVERY.name(),sysRecoveryPool ); 
		poolMap.put(TaskType.USER_DELETE.name(), userDelPool );
		poolMap.put(TaskType.SYSTEM_COPY_CHECK.name(), sysCopyPool );


		String createInveral = config.getProperty(CREATE_TASK_INTERVAL_TIME, "60000");
		long createTaskInveral = Long.valueOf(createInveral);
		conf.setCreateTaskIntervalTime(createTaskInveral);
		String createCopyTime = config.getProperty(SYSTEM_COPY_INTERVAL_TIME, "60000");
		long createCopytTimems = Long.valueOf(createCopyTime);
		conf.setCreateCheckJobTaskervalTime(createCopytTimems);
		
		String executeInveral = config.getProperty(EXECUTE_TASK_INTERVAL_TIME, "60000");
		long executeTaskInveral = Long.valueOf(executeInveral);
		conf.setExecuteTaskIntervalTime(executeTaskInveral);
		
		String gatherInveral = config.getProperty(GATHER_RESOURCE_INVERAL_TIME, "60000");
		long gatherResourceInveral = Long.valueOf(gatherInveral);
		conf.setGatherResourceInveralTime(gatherResourceInveral);
		
		String calcCount = config.getProperty(CALC_RESOURCE_VALUE_COUNT, "5");
		int calcResourceCount = Integer.valueOf(calcCount);
		conf.setCalcResourceValueCount(calcResourceCount);
		String taskFrameWork = config.getProperty(TASK_FRAMEWORK_SWITCH,"true");
		boolean taskFrameWorkSwitch = Boolean.valueOf(taskFrameWork);
		conf.setTaskFrameWorkSwitch(taskFrameWorkSwitch);
		
		String resourceFrameWork = config.getProperty(RESOURCE_FRAMEWORK_SWITCH, "true");
		boolean resourceFrameWorkSwitch = Boolean.valueOf(resourceFrameWork);
		conf.setResourceFrameWorkSwitch(resourceFrameWorkSwitch);
		
		String libPath = System.getProperty(RESOURCE_LIB_PATH);
		if(BrStringUtils.isEmpty(libPath)){
			throw new NullPointerException(RESOURCE_LIB_PATH +" is empty");
		}
		conf.setLibPath(libPath);
		String expiredTime = config.getProperty(TASK_EXPIRED_TIME, "680400000");
		long expiredTaskTime = Long.valueOf(expiredTime);
		conf.setTaskExpiredTime(expiredTaskTime);
		
		String limtCpuRateStr = config.getProperty(LIMIT_RESOURCE_VALUE_CPU,"0.9");
		double limtCpuRate = Double.valueOf(limtCpuRateStr);
		conf.setLimitCpuRate(limtCpuRate);
		
		String limtMemoryRateStr = config.getProperty(LIMIT_RESOURCE_VALUE_MEMORY,"0.9");
		double limtMemoryRate = Double.valueOf(limtMemoryRateStr);
		conf.setLimitMemoryRate(limtMemoryRate);
		
		String limtDiskRemainStr = config.getProperty(LIMIT_RESOURCE_VALUE_DISK_REMAIN,"0.01");
		double limtDiskRemain = Double.valueOf(limtDiskRemainStr);
		conf.setLimitDiskRemaintRate(limtDiskRemain);
		
		String limtDiskWriteStr = config.getProperty(LIMIT_RESOURCE_VALUE_DISK_WRITE,"0.9");
		double limtDiskWrite = Double.valueOf(limtDiskWriteStr);
		conf.setLimitDiskWriteRate(limtDiskWrite);

		String limtDiskReadStr = config.getProperty(LIMIT_RESOURCE_VALUE_DISK_READ,"0.9");
		double limtDiskRead = Double.valueOf(limtDiskReadStr);
		conf.setLimitDiskReadRate(limtDiskRead);
		
		String limitNetTxStr = config.getProperty(LIMIT_RESOURCE_VALUE_NET_TX,"0.9");
		double limitNetTx = Double.valueOf(limitNetTxStr);
		conf.setLimitNetTxRate(limitNetTx);
		
		String limitNetRxStr = config.getProperty(LIMIT_RESOURCE_VALUE_NET_RX,"0.9");
		double limitNetRx = Double.valueOf(limitNetRxStr);
		conf.setLimitNetRxRate(limitNetRx);
		
	
		
		return conf;
	}
	public Map<String, Boolean> getTaskPoolSwitchMap() {
		return taskPoolSwitchMap;
	}
	public void setTaskPoolSwitchMap(Map<String, Boolean> taskPoolSwitchMap) {
		this.taskPoolSwitchMap = taskPoolSwitchMap;
	}
	public Map<String, Integer> getTaskPoolSizeMap() {
		return taskPoolSizeMap;
	}
	public void setTaskPoolSizeMap(Map<String, Integer> taskPoolSizeMap) {
		this.taskPoolSizeMap = taskPoolSizeMap;
	}
	public long getCreateTaskIntervalTime() {
		return createTaskIntervalTime;
	}
	public void setCreateTaskIntervalTime(long createTaskIntervalTime) {
		this.createTaskIntervalTime = createTaskIntervalTime;
	}
	public long getExecuteTaskIntervalTime() {
		return executeTaskIntervalTime;
	}
	public void setExecuteTaskIntervalTime(long executeTaskIntervalTime) {
		this.executeTaskIntervalTime = executeTaskIntervalTime;
	}
	public long getGatherResourceInveralTime() {
		return gatherResourceInveralTime;
	}
	public void setGatherResourceInveralTime(long gatherResourceInveralTime) {
		this.gatherResourceInveralTime = gatherResourceInveralTime;
	}
	public int getCalcResourceValueCount() {
		return calcResourceValueCount;
	}
	public void setCalcResourceValueCount(int calcResourceValueCount) {
		this.calcResourceValueCount = calcResourceValueCount;
	}
	public boolean isTaskFrameWorkSwitch() {
		return taskFrameWorkSwitch;
	}
	public void setTaskFrameWorkSwitch(boolean taskFrameWorkSwitch) {
		this.taskFrameWorkSwitch = taskFrameWorkSwitch;
	}
	public boolean isResourceFrameWorkSwitch() {
		return resourceFrameWorkSwitch;
	}
	public void setResourceFrameWorkSwitch(boolean resourceFrameWorkSwitch) {
		this.resourceFrameWorkSwitch = resourceFrameWorkSwitch;
	}
	public String getLibPath() {
		return libPath;
	}
	public void setLibPath(String libPath) {
		this.libPath = libPath;
	}
	public long getTaskExpiredTime() {
		return taskExpiredTime;
	}
	public void setTaskExpiredTime(long taskExpiredTime) {
		this.taskExpiredTime = taskExpiredTime;
	}
	public double getLimitCpuRate() {
		return limitCpuRate;
	}
	public void setLimitCpuRate(double limitCpuRate) {
		this.limitCpuRate = limitCpuRate;
	}
	public double getLimitMemoryRate() {
		return limitMemoryRate;
	}
	public void setLimitMemoryRate(double limitMemoryRate) {
		this.limitMemoryRate = limitMemoryRate;
	}
	public double getLimitDiskRemaintRate() {
		return limitDiskRemaintRate;
	}
	public void setLimitDiskRemaintRate(double limitDiskRemaintRate) {
		this.limitDiskRemaintRate = limitDiskRemaintRate;
	}
	public double getLimitDiskReadRate() {
		return limitDiskReadRate;
	}
	public void setLimitDiskReadRate(double limitDiskReadRate) {
		this.limitDiskReadRate = limitDiskReadRate;
	}
	public double getLimitDiskWriteRate() {
		return limitDiskWriteRate;
	}
	public void setLimitDiskWriteRate(double limitDiskWriteRate) {
		this.limitDiskWriteRate = limitDiskWriteRate;
	}
	public double getLimitNetTxRate() {
		return limitNetTxRate;
	}
	public void setLimitNetTxRate(double limitNetTxRate) {
		this.limitNetTxRate = limitNetTxRate;
	}
	public double getLimitNetRxRate() {
		return limitNetRxRate;
	}
	public void setLimitNetRxRate(double limitNetRxRate) {
		this.limitNetRxRate = limitNetRxRate;
	}
	public long getCreateCheckJobTaskervalTime() {
		return createCheckJobTaskervalTime;
	}
	public void setCreateCheckJobTaskervalTime(long createCheckJobTaskervalTime) {
		this.createCheckJobTaskervalTime = createCheckJobTaskervalTime;
	}
	 
}
