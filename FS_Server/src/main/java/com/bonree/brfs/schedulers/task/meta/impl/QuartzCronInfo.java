package com.bonree.brfs.schedulers.task.meta.impl;

import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import java.util.HashMap;
import java.util.Map;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月28日 下午3:57:13
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: Quartz Cron任务信息
 *****************************************************************************
 */
public class QuartzCronInfo implements SumbitTaskInterface {
    private String taskName;
    private String taskGroupName;
    private String classInstanceName;
    private Map<String, String> taskContent = new HashMap<String, String>();
    private String cronTime;
    private int taskKind = 0;

    public void putContent(String key, String value) {
        this.taskContent.put(key, value);
    }

    @Override
    public String getTaskName() {
        return this.taskName;
    }

    @Override
    public String getTaskGroupName() {
        return this.taskGroupName;
    }

    @Override
    public String getClassInstanceName() {
        return this.classInstanceName;
    }

    @Override
    public String getCycleContent() {
        return this.cronTime;
    }

    @Override
    public Map<String, String> getTaskContent() {
        return taskContent;
    }

    @Override
    public int getTaskKind() {
        return this.taskKind;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setTaskGroupName(String taskGroupName) {
        this.taskGroupName = taskGroupName;
    }

    public void setClassInstanceName(String classInstanceName) {
        this.classInstanceName = classInstanceName;
    }

    public void setTaskContent(Map<String, String> taskContent) {
        this.taskContent = taskContent;
    }

    public void setCronTime(String cronTime) {
        this.cronTime = cronTime;
    }

    public static QuartzCronInfo getInstance(String taskName, String group, String cronStr, Map<String, String> cronMap,
                                             Class<?> clazz) {
        QuartzCronInfo cron = new QuartzCronInfo();
        cron.setTaskName(taskName);
        cron.setTaskGroupName(group);
        cron.setCronTime(cronStr);
        cron.setClassInstanceName(clazz.getCanonicalName());
        if ((cronMap != null) && (!cronMap.isEmpty())) {
            cron.setTaskContent(cronMap);
        }
        return cron;
    }
}
