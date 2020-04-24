package com.bonree.brfs.configuration;

import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger("ResourceTaskConfig");

    /**
     * 任务开关
     */
    Map<String, Boolean> taskPoolSwitchMap = new ConcurrentHashMap<String, Boolean>();
    /**
     * 任务线程池大小
     */
    Map<String, Integer> taskPoolSizeMap = new ConcurrentHashMap<String, Integer>();

    //创建任务执行的时间间隔s
    private long createTaskIntervalTime = 60;
    private long executeTaskIntervalTime = 60;
    private long gatherResourceInveralTime = 10;
    private long taskExpiredTime = 7 * 24 * 60 * 60;
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
    private long createCheckJobTaskervalTime = 60;
    private long checkTtl = 24 * 60 * 60;

    private String checkCronStr = "0 30 2 * * ?";
    private int checkTimeRange = 7;

    private String watchDogCron = "0 30 2 */7 * ?";

    public ResourceTaskConfig() {
    }

    public static ResourceTaskConfig parse() throws NullPointerException, ConfigParseException {
        ResourceTaskConfig conf = new ResourceTaskConfig();

        ConfigObj config = Configs.getConfiguration();
        if (config == null) {
            throw new NullPointerException("configuration is empty !!");
        }
        Map<String, Boolean> configMap = conf.getTaskPoolSwitchMap();
        boolean sysDelFlag = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_DELETE);
        boolean sysMergeFlag = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_MERGE);
        boolean sysCheckFlag = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_CHECK);
        boolean sysRecoveryFlag = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_RECOVER);
        boolean userDelFlag = config.getConfig(ResourceConfigs.CONFIG_USER_DELETE);
        boolean sysCopyFlag = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_COPY);
        configMap.put(TaskType.SYSTEM_DELETE.name(), sysDelFlag);
        configMap.put(TaskType.SYSTEM_MERGER.name(), sysMergeFlag);
        configMap.put(TaskType.SYSTEM_CHECK.name(), sysCheckFlag);
        configMap.put(TaskType.USER_DELETE.name(), userDelFlag);
        configMap.put(TaskType.SYSTEM_COPY_CHECK.name(), sysCopyFlag);

        Map<String, Integer> poolMap = conf.getTaskPoolSizeMap();

        int sysDelPool = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_DELETE_SIZE);
        int sysMergePool = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_MERGE_SIZE);
        int sysCheckPool = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_CHECK_SIZE);
        int sysRecoveryPool = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_RECOVER_SIZE);
        int userDelPool = config.getConfig(ResourceConfigs.CONFIG_USER_DELETE_SIZE);
        int sysCopyPool = config.getConfig(ResourceConfigs.CONFIG_SYSTEM_COPY_SIZE);

        poolMap.put(TaskType.SYSTEM_DELETE.name(), sysDelPool);
        poolMap.put(TaskType.SYSTEM_MERGER.name(), sysMergePool);
        poolMap.put(TaskType.SYSTEM_CHECK.name(), sysCheckPool);
        poolMap.put(TaskType.USER_DELETE.name(), userDelPool);
        poolMap.put(TaskType.SYSTEM_COPY_CHECK.name(), sysCopyPool);

        conf.setCreateTaskIntervalTime(TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_TASK_CREATE_INTERVAL)));
        conf.setCreateCheckJobTaskervalTime(
            TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_COPY_CHECK_CREATE_INTERVAL)));

        conf.setExecuteTaskIntervalTime(
            TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_TASK_EXECUTE_INTERVAL)));

        conf.setGatherResourceInveralTime(
            TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_RESOURCE_GATHER_INTERVAL)));

        conf.setCalcResourceValueCount(config.getConfig(ResourceConfigs.CONFIG_RESOURCE_CALCULATE_COUNT));
        conf.setTaskFrameWorkSwitch(config.getConfig(ResourceConfigs.CONFIG_TASK_ENABLE));

        conf.setResourceFrameWorkSwitch(config.getConfig(ResourceConfigs.CONFIG_RESOURCE_ENABLE));

        String libPath = System.getProperty(SystemProperties.PROP_RESOURCE_LIB_PATH);
        if (BrStringUtils.isEmpty(libPath)) {
            throw new NullPointerException(SystemProperties.PROP_RESOURCE_LIB_PATH + " is empty");
        }
        conf.setLibPath(libPath);
        conf.setTaskExpiredTime(TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_TASK_EXPIRED_TIME)));

        conf.setLimitCpuRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_CPU_RATE));
        conf.setLimitMemoryRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_MEM_RATE));
        conf.setLimitDiskRemaintRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_DISK_AVAILABLE_RATE));
        conf.setLimitDiskWriteRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_DISK_WRITE_SPEED));
        conf.setLimitDiskReadRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_DISK_READ_SPEED));
        conf.setLimitNetTxRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_NET_SEND));
        conf.setLimitNetRxRate(config.getConfig(ResourceConfigs.CONFIG_LIMIT_NET_RECEIVE));
        //TODO 测试阶段，改字段改为s
        conf.setCheckTtl(TimeUnit.SECONDS.toMillis(config.getConfig(ResourceConfigs.CONFIG_DATA_CHECK_TTL)));

        String cronStr = analyseCronStr(config.getConfig(ResourceConfigs.CONFIG_SCHED_COPY_CHECK_CLOCK), 0);
        String tmpCronStr = config.getConfig(ResourceConfigs.CONFIG_TEST_COUNT_CRON_STR);
        if (BrStringUtils.isEmpty(tmpCronStr)) {
            conf.setCheckCronStr(cronStr);
        } else {
            conf.setCheckCronStr(tmpCronStr);
        }

        int day = config.getConfig(ResourceConfigs.CONFIG_SCHED_COPY_CHECK_RANGE);
        if (day <= 0) {
            throw new ConfigParseException("cycle.check.copy.count.time.range : " + day + " is error!! please check it");
        }
        conf.setCheckTimeRange(day);
        String watchTime = config.getConfig(ResourceConfigs.CONFIG_SCHED_WATCHDOG_TRIGGER_CLOCK);
        int watchInt = config.getConfig(ResourceConfigs.CONFIG_SCHED_WATCHDOG_TRIGGER_INTERVAL);
        String watchCron = analyseCronStr(watchTime, watchInt);
        conf.setWatchDogCron(watchCron);
        return conf;
    }

    private static String analyseCronStr(String content, int interval) throws ConfigParseException {
        String[] times = BrStringUtils.getSplit(content, ":");
        if ((times == null)
            || (times.length != 2)
            || (!BrStringUtils.isNumeric(times[0]))
            || (!BrStringUtils.isNumeric(times[1]))) {
            throw new ConfigParseException("cycle.check.copy.count.time : " + content + " is error!! please check it");
        }
        int ihour = Integer.parseInt(times[0]);
        int imin = Integer.parseInt(times[1]);
        if ((ihour < 0) || (ihour >= 24) || (imin < 0) || (imin >= 60)) {
            throw new ConfigParseException("cycle.check.copy.count.time : " + content + " is error!! please check it");
        }
        if (interval <= 0) {
            return "0 " + imin + " " + ihour + " * * ?";
        } else {
            return "0 " + imin + " " + ihour + " */" + interval + " * ?";
        }
    }

    public void printDetail() {
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_DELETE.name(),
                 this.taskPoolSwitchMap.get(TaskType.SYSTEM_DELETE.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_CHECK.name(), this.taskPoolSwitchMap.get(TaskType.SYSTEM_CHECK.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_COPY.name(),
                 this.taskPoolSwitchMap.get(TaskType.SYSTEM_COPY_CHECK.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_USER_DELETE.name(), this.taskPoolSwitchMap.get(TaskType.USER_DELETE.name()));

        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_DELETE_SIZE.name(),
                 this.taskPoolSizeMap.get(TaskType.SYSTEM_DELETE.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_CHECK_SIZE.name(),
                 this.taskPoolSizeMap.get(TaskType.SYSTEM_CHECK.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_SYSTEM_COPY_SIZE.name(),
                 this.taskPoolSizeMap.get(TaskType.SYSTEM_COPY_CHECK.name()));
        LOG.info("{} :{} ", ResourceConfigs.CONFIG_USER_DELETE_SIZE.name(),
                 this.taskPoolSizeMap.get(TaskType.USER_DELETE.name()));
        LOG.info("{}:{}", ResourceConfigs.CONFIG_RESOURCE_CALCULATE_COUNT.name(), this.calcResourceValueCount);
        LOG.info("{}:{} ms", ResourceConfigs.CONFIG_DATA_CHECK_TTL.name(), this.checkTtl);
        LOG.info("{}:{} ms", ResourceConfigs.CONFIG_COPY_CHECK_CREATE_INTERVAL.name(), this.createCheckJobTaskervalTime);
        LOG.info("{}:{} ms", ResourceConfigs.CONFIG_TASK_EXECUTE_INTERVAL.name(), this.executeTaskIntervalTime);
        LOG.info("{}:{} ms", ResourceConfigs.CONFIG_TASK_CREATE_INTERVAL.name(), this.createTaskIntervalTime);
        LOG.info("{}:{} ms", ResourceConfigs.CONFIG_RESOURCE_GATHER_INTERVAL.name(), this.gatherResourceInveralTime);
        LOG.info("{}:{} d", ResourceConfigs.CONFIG_TASK_EXPIRED_TIME.name(), this.taskExpiredTime / 1000 / 60 / 60 / 24);
        LOG.info("{}:{}", SystemProperties.PROP_RESOURCE_LIB_PATH, this.libPath);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_NET_RECEIVE.name(), this.limitNetRxRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_NET_SEND.name(), this.limitNetTxRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_CPU_RATE.name(), this.limitCpuRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_DISK_READ_SPEED.name(), this.limitDiskReadRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_DISK_AVAILABLE_RATE.name(), this.limitDiskRemaintRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_DISK_WRITE_SPEED.name(), this.limitDiskWriteRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_LIMIT_MEM_RATE.name(), this.limitMemoryRate);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_RESOURCE_ENABLE.name(), this.resourceFrameWorkSwitch);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_TASK_ENABLE.name(), this.taskFrameWorkSwitch);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_SCHED_COPY_CHECK_CLOCK.name(), this.checkCronStr);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_SCHED_COPY_CHECK_RANGE.name(), this.checkTimeRange);
        LOG.info("{}:{}", ResourceConfigs.CONFIG_SCHED_WATCHDOG_TRIGGER_CLOCK.name(), this.watchDogCron);
    }

    public Map<String, Boolean> getTaskPoolSwitchMap() {
        return taskPoolSwitchMap;
    }

    public List<TaskType> getSwitchOnTaskType() {
        List<TaskType> collect = new ArrayList<TaskType>();
        if (taskPoolSwitchMap == null || taskPoolSwitchMap.isEmpty()) {
            return null;
        }
        String task = null;
        TaskType taskType = null;
        for (Map.Entry<String, Boolean> entry : this.taskPoolSwitchMap.entrySet()) {
            task = entry.getKey();
            if (entry.getValue() && !BrStringUtils.isEmpty(task)) {
                taskType = TaskType.valueOf(task);
                if (taskType != null) {
                    collect.add(taskType);
                }
            }
        }
        return collect;
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

    public long getCheckTtl() {
        return checkTtl;
    }

    public void setCheckTtl(long checkTtl) {
        this.checkTtl = checkTtl;
    }

    public String getCheckCronStr() {
        return checkCronStr;
    }

    public void setCheckCronStr(String checkCronStr) {
        this.checkCronStr = checkCronStr;
    }

    public int getCheckTimeRange() {
        return checkTimeRange;
    }

    public void setCheckTimeRange(int checkTimeRange) {
        this.checkTimeRange = checkTimeRange;
    }

    public String getWatchDogCron() {
        return watchDogCron;
    }

    public void setWatchDogCron(String watchDogCron) {
        this.watchDogCron = watchDogCron;
    }
}
