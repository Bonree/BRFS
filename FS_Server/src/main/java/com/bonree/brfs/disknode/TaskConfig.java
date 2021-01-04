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
    private int sysDeleteSize = 3;
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
    private int fileBlockScanIntervalMinute = 1440;

    @JsonProperty("file.block.start.delay.minute")
    private int startdelayMinute = 10;

    @JsonProperty("second.register.interval.millisecond")
    private int secondIdRegisterIntervalMill = 1000;

    @JsonProperty("trash.can.scan.interval.minute")
    private int trashCanScanIntervalMinute = 1440;

    @JsonProperty("trash.can.start.delay.minute")
    private int trashCanDelayMinute = 10;

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
        logger.info("config item: task.{}, value: {}", "file.block.scan.interval.minute", fileBlockScanIntervalMinute);
        logger.info("config item: task.{}, value: {}", "second.register.interval.millisecond", secondIdRegisterIntervalMill);
        logger.info("config item: task.{}, value: {}", "file.block.start.delay.minute", startdelayMinute);
        logger.info("config item: task.{}, value: {}", "trash.can.start.delay.minute", trashCanDelayMinute);
        logger.info("config item: task.{}, value: {}", "trash.can.scan.interval.minute", trashCanScanIntervalMinute);
    }

    public List<String> getTaskSwitch() {
        return taskSwitch;
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

    public int getUserDeleteSize() {
        return userDeleteSize;
    }

    public int getSysCheckSize() {
        return sysCheckSize;
    }

    public int getSysCopySize() {
        return sysCopySize;
    }

    public int getCommonDelayTimeSecond() {
        return commonDelayTimeSecond;
    }

    public int getCommonCreateIntervalSecond() {
        return commonCreateIntervalSecond;
    }

    public String getCycleCopyTimeStr() {
        return cycleCopyTimeStr;
    }

    public int getCycleCopyRangeDay() {
        return cycleCopyRangeDay;
    }

    public long getTtlSecond() {
        return ttlSecond;
    }

    public String getFileBlockScanTime() {
        return fileBlockScanTime;
    }

    public int getFileBlockScanIntervalMinute() {
        return fileBlockScanIntervalMinute;
    }

    public int getCommonExecuteIntervalSecond() {
        return commonExecuteIntervalSecond;
    }

    public int getStartdelayMinute() {
        return startdelayMinute;
    }

    public int getTrashCanScanIntervalMinute() {
        return trashCanScanIntervalMinute;
    }

    public int getTrashCanDelayMinute() {
        return trashCanDelayMinute;
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

}
