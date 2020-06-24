package com.bonree.brfs.disknode;

import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.slf4j.Logger;

/**
 * 任务配置信息
 */
public class TaskConfig {
    @JsonProperty("switch.set")
    private List<String> taskSwitch = TaskType.getDefaultTaskString();
    @JsonProperty("system.delete.pool.size")
    private int sysDeleteSize = 1;
    @JsonProperty("user.delete.pool.size")
    private int userDeleteSize = 1;
    @JsonProperty("system.check.pool.size")
    private int sysCheckSize = 1;
    @JsonProperty("system.copy.pool.size")
    private int sysCopySize = 1;

    @JsonProperty("common.create.delay.second")
    private int commonDelayTimeSecond = 60;
    @JsonProperty("common.create.interval.second")
    private int commonCreateIntervalSecond = 60;
    @JsonProperty("common.execute.interval.second")
    private int commonExecuteIntervalSecond = 30;
    @JsonProperty("cycle.check.copy.count.time")
    private String cycleCopyTimeStr = "2:30";

    @JsonProperty("cycle.check.copy.count.time.range")
    private int cycleCopyRangeDay = 7;

    @JsonProperty("ttl.second")
    private long ttlSecond = 604800L;

    @JsonProperty("file.block.scan.time")
    private String fileBlockScanTime = "02:00";

    @JsonProperty("file.block.scan.interval.minute")
    private int fileBlockScanIntervalSecond = 1440;

    @JsonProperty("second.register.interval.millisecond")
    private int secondIdRegisterIntervalMill = 1000;

    public void printDetail(Logger logger) {
        logger.info("config item: task.{}, value: {}", "switch.set", taskSwitch);
        logger.info("config item: task.{}, value: {}", "system.delete.pool.size", sysDeleteSize);
        logger.info("config item: task.{}, value: {}", "user.delete.pool.size", userDeleteSize);
        logger.info("config item: task.{}, value: {}", "system.check.pool.size", sysCheckSize);
        logger.info("config item: task.{}, value: {}", "system.copy.pool.size", sysCopySize);
        logger.info("config item: task.{}, value: {}", "common.create.delay.second", commonDelayTimeSecond);
        logger.info("config item: task.{}, value: {}", "common.create.interval.second", commonCreateIntervalSecond);
        logger.info("config item: task.{}, value: {}", "common.execute.interval.second", commonExecuteIntervalSecond);
        logger.info("config item: task.{}, value: {}", "cycle.check.copy.count.time", cycleCopyTimeStr);
        logger.info("config item: task.{}, value: {}", "cycle.check.copy.count.time.range", cycleCopyRangeDay);
        logger.info("config item: task.{}, value: {}", "ttl.second", ttlSecond);
        logger.info("config item: task.{}, value: {}", "file.block.scan.time", fileBlockScanTime);
        logger.info("config item: task.{}, value: {}", "file.block.scan.interval.minute", fileBlockScanIntervalSecond);
        logger.info("config item: task.{}, value: {}", "second.register.interval.millisecond", secondIdRegisterIntervalMill);
    }

    public List<String> getTaskSwitch() {
        return taskSwitch;
    }

    public void setTaskSwitch(List<String> taskSwitch) {
        this.taskSwitch = taskSwitch;
    }

    @JsonIgnore
    public Collection<TaskType> getTaskTypeSwitch() {
        if (this.taskSwitch == null) {
            return ImmutableList.of();
        }
        Collection<TaskType> taskTypes = new HashSet<>();
        this.taskSwitch.forEach(task -> {
            TaskType type = TaskType.valueOf(task);
            if (type != null && !TaskType.UNDEFINE.equals(type)) {
                taskTypes.add(type);
            }
        });
        return taskTypes;
    }

    public int getSysDeleteSize() {
        return sysDeleteSize;
    }

    public void setSysDeleteSize(int sysDeleteSize) {
        this.sysDeleteSize = sysDeleteSize;
    }

    public int getUserDeleteSize() {
        return userDeleteSize;
    }

    public void setUserDeleteSize(int userDeleteSize) {
        this.userDeleteSize = userDeleteSize;
    }

    public int getSysCheckSize() {
        return sysCheckSize;
    }

    public void setSysCheckSize(int sysCheckSize) {
        this.sysCheckSize = sysCheckSize;
    }

    public int getSysCopySize() {
        return sysCopySize;
    }

    public void setSysCopySize(int sysCopySize) {
        this.sysCopySize = sysCopySize;
    }

    public int getCommonDelayTimeSecond() {
        return commonDelayTimeSecond;
    }

    public void setCommonDelayTimeSecond(int commonDelayTimeSecond) {
        this.commonDelayTimeSecond = commonDelayTimeSecond;
    }

    public int getCommonCreateIntervalSecond() {
        return commonCreateIntervalSecond;
    }

    public void setCommonCreateIntervalSecond(int commonCreateIntervalSecond) {
        this.commonCreateIntervalSecond = commonCreateIntervalSecond;
    }

    public String getCycleCopyTimeStr() {
        return cycleCopyTimeStr;
    }

    public void setCycleCopyTimeStr(String cycleCopyTimeStr) {
        this.cycleCopyTimeStr = cycleCopyTimeStr;
    }

    public int getCycleCopyRangeDay() {
        return cycleCopyRangeDay;
    }

    public void setCycleCopyRangeDay(int cycleCopyRangeDay) {
        this.cycleCopyRangeDay = cycleCopyRangeDay;
    }

    public long getTtlSecond() {
        return ttlSecond;
    }

    public void setTtlSecond(long ttlSecond) {
        this.ttlSecond = ttlSecond;
    }

    public String getFileBlockScanTime() {
        return fileBlockScanTime;
    }

    public void setFileBlockScanTime(String fileBlockScanTime) {
        this.fileBlockScanTime = fileBlockScanTime;
    }

    public int getFileBlockScanIntervalSecond() {
        return fileBlockScanIntervalSecond;
    }

    public void setFileBlockScanIntervalSecond(int fileBlockScanIntervalSecond) {
        this.fileBlockScanIntervalSecond = fileBlockScanIntervalSecond;
    }

    public int getCommonExecuteIntervalSecond() {
        return commonExecuteIntervalSecond;
    }

    public void setCommonExecuteIntervalSecond(int commonExecuteIntervalSecond) {
        this.commonExecuteIntervalSecond = commonExecuteIntervalSecond;
    }

    /**
     * 获取配置信息
     *
     * @return
     */
    @JsonIgnore
    public String getCronStr() throws Exception {
        return analyseCronStr(getCycleCopyTimeStr(), 0);
    }

    private String analyseCronStr(String content, int interval) throws ConfigParseException {
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

    public int getSecondIdRegisterIntervalMill() {
        return secondIdRegisterIntervalMill;
    }

    public void setSecondIdRegisterIntervalMill(int secondIdRegisterIntervalMill) {
        this.secondIdRegisterIntervalMill = secondIdRegisterIntervalMill;
    }
}
