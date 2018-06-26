package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class ResourceConfigs {
	public static final ConfigUnit<Boolean> CONFIG_SYSTEM_DELETE =
			ConfigUnit.ofBoolean("system.delete.pool.switch", true);
	
	public static final ConfigUnit<Integer> CONFIG_SYSTEM_DELETE_SIZE =
			ConfigUnit.ofInt("system.delete.pool.size", 1);
	
	public static final ConfigUnit<Boolean> CONFIG_SYSTEM_MERGE =
			ConfigUnit.ofBoolean("system.merge.pool.switch", false);
	
	public static final ConfigUnit<Integer> CONFIG_SYSTEM_MERGE_SIZE =
			ConfigUnit.ofInt("system.merge.pool.size", 1);
	
	public static final ConfigUnit<Boolean> CONFIG_SYSTEM_CHECK =
			ConfigUnit.ofBoolean("system.check.pool.switch", false);
	
	public static final ConfigUnit<Integer> CONFIG_SYSTEM_CHECK_SIZE =
			ConfigUnit.ofInt("system.check.pool.size", 1);
	
	public static final ConfigUnit<Boolean> CONFIG_SYSTEM_RECOVER =
			ConfigUnit.ofBoolean("system.recovery.pool.switch", false);
	
	public static final ConfigUnit<Integer> CONFIG_SYSTEM_RECOVER_SIZE =
			ConfigUnit.ofInt("system.recovery.pool.size", 1);
	
	public static final ConfigUnit<Boolean> CONFIG_SYSTEM_COPY =
			ConfigUnit.ofBoolean("system.copy.pool.switch", false);
	
	public static final ConfigUnit<Integer> CONFIG_SYSTEM_COPY_SIZE =
			ConfigUnit.ofInt("system.copy.pool.size", 1);
	
	public static final ConfigUnit<Boolean> CONFIG_USER_DELETE =
			ConfigUnit.ofBoolean("user.delete.pool.switch", true);
	
	public static final ConfigUnit<Integer> CONFIG_USER_DELETE_SIZE =
			ConfigUnit.ofInt("user.delete.pool.size", 1);
	
	public static final ConfigUnit<Long> CONFIG_TASK_CREATE_INTERVAL =
			ConfigUnit.ofLong("system.create.task.inverval.time", 60);
	
	public static final ConfigUnit<Long> CONFIG_COPY_CHECK_CREATE_INTERVAL =
			ConfigUnit.ofLong("system.copy.check.create.inveratal.time", 60);
	
	public static final ConfigUnit<Long> CONFIG_TASK_EXECUTE_INTERVAL =
			ConfigUnit.ofLong("execute.task.inverval.time", 60l);
	
	public static final ConfigUnit<Long> CONFIG_RESOURCE_GATHER_INTERVAL =
			ConfigUnit.ofLong("gather.resource.inveral.time", 60l);
	
	public static final ConfigUnit<Integer> CONFIG_RESOURCE_CALCULATE_COUNT =
			ConfigUnit.ofInt("calc.resource.value.count", 5);
	
	public static final ConfigUnit<Boolean> CONFIG_TASK_ENABLE =
			ConfigUnit.ofBoolean("task.framework.switch", true);
	
	public static final ConfigUnit<Boolean> CONFIG_RESOURCE_ENABLE =
			ConfigUnit.ofBoolean("resource.framework.switch", true);
	
	public static final ConfigUnit<Long> CONFIG_TASK_EXPIRED_TIME =
			ConfigUnit.ofLong("task.expired.time", 680400l);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_CPU_RATE =
			ConfigUnit.ofDouble("limit.resource.value.cpurate", 0.9);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_MEM_RATE =
			ConfigUnit.ofDouble("limit.resource.value.memoryrate", 0.9);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_DISK_AVAILABLE_RATE =
			ConfigUnit.ofDouble("limit.resource.value.disakremainrate", 0.01);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_DISK_WRITE_SPEED =
			ConfigUnit.ofDouble("limit.resource.value.diskwritespeedrate", 0.9);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_DISK_READ_SPEED =
			ConfigUnit.ofDouble("limit.resource.value.diskreadspeedrate", 0.9);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_NET_SEND =
			ConfigUnit.ofDouble("limit.resource.value.nettspeedrate", 0.9);
	
	public static final ConfigUnit<Double> CONFIG_LIMIT_NET_RECEIVE =
			ConfigUnit.ofDouble("limit.resource.value.netrspeedrate", 0.9);
	
	public static final ConfigUnit<Long> CONFIG_DATA_CHECK_TTL =
			ConfigUnit.ofLong("system.check.data.ttl", 3600);
	
	public static final ConfigUnit<String> CONFIG_SCHED_COPY_CHECK_CLOCK =
			ConfigUnit.ofString("cycle.check.copy.count.time", "2:30");
	
	public static final ConfigUnit<Integer> CONFIG_SCHED_COPY_CHECK_RANGE =
			ConfigUnit.ofInt("cycle.check.copy.count.time.range", 7);
	
	public static final ConfigUnit<String> CONFIG_SCHED_WATCHDOG_TRIGGER_CLOCK =
			ConfigUnit.ofString("watchdog.trigger.time", "2:30");
	
	public static final ConfigUnit<Integer> CONFIG_SCHED_WATCHDOG_TRIGGER_INTERVAL =
			ConfigUnit.ofInt("watch.dog.trigger.interval", 7);

	private ResourceConfigs() {}
}
