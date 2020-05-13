package com.bonree.brfs.schedulers.task.manager.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorConfig;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.biz.CopyRecoveryJob;
import com.bonree.brfs.schedulers.task.manager.BaseSchedulerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.mail.worker.MailWorker;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月28日 下午4:19:33
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 单例模式的调度接口
 *****************************************************************************
 */
@ManageLifecycle
public class DefaultSchedulersManager implements SchedulerManagerInterface<String, BaseSchedulerInterface, SumbitTaskInterface> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulersManager.class);
    private Map<String, BaseSchedulerInterface> taskPoolMap = new ConcurrentHashMap<String, BaseSchedulerInterface>();
    private ResourceTaskConfig config;
    private Service service;
    private ZookeeperPaths zookeeperPaths;
    private CuratorConfig curatorConfig;

    @Inject
    public DefaultSchedulersManager(
        CuratorConfig curatorConfig, ResourceTaskConfig config, Service service,
        ZookeeperPaths zookeeperPaths) {
        this.curatorConfig = curatorConfig;
        this.config = config;
        this.service = service;
        this.zookeeperPaths = zookeeperPaths;
    }

    @Override
    public boolean addTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException {
        checkParams(taskpoolkey, task);
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
        if (pool == null) {
            return false;
        }
        try {
            return pool.addTask(task);
        } catch (Exception e) {
            LOG.error("add task error {},{},{}", taskpoolkey, task.getClassInstanceName(), e);
            EmailPool emailPool = EmailPool.getInstance();
            MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
            builder.setMessage("添加" + taskpoolkey + "任务发生异常 ！！");
            builder.setVariable(task.getTaskContent());
            builder.setException(e);
            builder.setModel(this.getClass().getSimpleName());
            emailPool.sendEmail(builder);

            return false;
        }
    }

    @Override
    public boolean pauseTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException {
        checkParams(taskpoolkey, task);
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
        if (pool == null) {
            return false;
        }
        try {
            pool.pauseTask(task);
        } catch (Exception e) {
            LOG.error("pause task error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean resumeTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException {
        checkParams(taskpoolkey, task);
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
        if (pool == null) {
            return false;
        }
        try {
            pool.resumeTask(task);
        } catch (Exception e) {
            LOG.error("resume task error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteTask(String taskpoolkey, SumbitTaskInterface task) throws ParamsErrorException {
        checkParams(taskpoolkey, task);
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolkey);
        if (pool == null) {
            return false;
        }
        try {
            pool.deleteTask(task);
        } catch (Exception e) {
            LOG.error("delete task error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean createTaskPool(String taskpoolKey, Properties prop) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (taskPoolMap.get(taskpoolKey) != null) {
            return true;
        }
        BaseSchedulerInterface pool = new DefaultBaseSchedulers();
        String name = prop.getProperty("org.quartz.scheduler.instanceName");
        if (BrStringUtils.isEmpty(name)) {
            prop.setProperty("org.quartz.scheduler.instanceName", taskpoolKey);
        }
        try {
            pool.initProperties(prop);
            this.taskPoolMap.put(taskpoolKey, pool);
        } catch (Exception e) {
            LOG.error("create task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean startTaskPool(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            if (pool.isStart()) {
                return false;
            }
            pool.start();
        } catch (Exception e) {
            LOG.error("start task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean reStartTaskPool(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            pool.reStart();
        } catch (Exception e) {
            LOG.error("restart task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean pauseTaskPool(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            pool.pausePool();
        } catch (Exception e) {
            LOG.error("pauset task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean resumeTaskPool(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            pool.resumePool();
        } catch (Exception e) {
            LOG.error("resume task pool error {}", e);
            return false;
        }
        return true;
    }

    private void checkParams(String taskpoolKey, SumbitTaskInterface task) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (task == null) {
            throw new ParamsErrorException("task is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key : " + taskpoolKey + " is not exists !!!");
        }
    }

    @Override
    public Collection<String> getAllPoolKey() {
        return this.taskPoolMap.keySet();
    }

    @Override
    public Collection<String> getStartedPoolKeys() {
        return getState(0);
    }

    @Override
    public Collection<String> getClosedPoolKeys() {
        return getState(2);
    }

    @Override
    public Collection<String> getPausePoolKeys() {
        return getState(1);
    }

    private Collection<String> getState(int type) {
        List<String> set = new ArrayList<String>();
        if (this.taskPoolMap.isEmpty()) {
            return set;
        }
        String key = null;
        BaseSchedulerInterface tmp = null;
        for (Map.Entry<String, BaseSchedulerInterface> entry : this.taskPoolMap.entrySet()) {
            key = entry.getKey();
            tmp = entry.getValue();
            if (type == 0 && tmp.isStart()) {
                set.add(key);
            } else if (type == 1 && tmp.isPaused()) {
                set.add(key);
            } else if (type == 2 && tmp.isDestory()) {
                set.add(key);
            }
        }
        return set;
    }

    @Override
    public boolean isStarted(String taskpoolKey) throws ParamsErrorException {
        return getState(taskpoolKey, 0);
    }

    @Override
    public boolean isClosed(String taskpoolKey) throws ParamsErrorException {
        return getState(taskpoolKey, 2);
    }

    @Override
    public boolean isPaused(String taskpoolKey) throws ParamsErrorException {
        return getState(taskpoolKey, 1);
    }

    private boolean getState(String taskpoolKey, int type) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key : " + taskpoolKey + " is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            throw new ParamsErrorException("task pool key : " + taskpoolKey + " is not exists !!!");
        }
        if (type == 0) {
            return pool.isStart();
        } else if (type == 1) {
            return pool.isPaused();
        } else if (type == 2) {
            return pool.isDestory();
        }
        return false;

    }

    @Override
    public int getTaskStat(String taskpoolKey, SumbitTaskInterface task) throws ParamsErrorException {
        checkParams(taskpoolKey, task);
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return -2;
        }
        return pool.getTaskStat(task);
    }

    @Override
    public boolean destoryTaskPool(String taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return true;
        }
        try {
            pool.close(isWaitTaskCompleted);
            this.taskPoolMap.remove(taskpoolKey);
        } catch (Exception e) {
            LOG.error("destory task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean closeTaskPool(String taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return true;
        }
        try {
            pool.close(isWaitTaskCompleted);
        } catch (Exception e) {
            LOG.error("closet task pool error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean pauseAllTask(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            pool.pauseAllTask();
        } catch (Exception e) {
            LOG.error("pause all task error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean resumeAllTask(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is empty !!!");
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            throw new ParamsErrorException("task pool key is not exists !!!");
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            return false;
        }
        try {
            pool.resumeAllTask();
        } catch (Exception e) {
            LOG.error("resume task error {}", e);
            return false;
        }
        return true;
    }

    @Override
    public int getSumbitedTaskCount(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            LOG.warn("taskpoolKey is empty");
            return 0;
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            LOG.warn("{} is not exists", taskpoolKey);
            return 0;
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            LOG.warn("{}' thread pool is null");
            return 0;
        }
        try {
            return pool.getSumbitTaskCount();
        } catch (Exception e) {
            LOG.error("get sumbit task count error {}", e);
            return 0;
        }
    }

    @Override
    public int getTaskPoolSize(String taskpoolKey) throws ParamsErrorException {
        if (BrStringUtils.isEmpty(taskpoolKey)) {
            LOG.warn("taskpoolKey is empty");
            return 0;
        }
        if (!taskPoolMap.containsKey(taskpoolKey)) {
            LOG.warn("{} is not exists", taskpoolKey);
            return 0;
        }
        BaseSchedulerInterface pool = taskPoolMap.get(taskpoolKey);
        if (pool == null) {
            LOG.warn("{}' thread pool is null");
            return 0;
        }
        try {
            return pool.getPoolSize();
        } catch (Exception e) {
            LOG.error("get task pool size error {}", e);
            return 0;
        }
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        Map<String, Boolean> switchMap = config.getTaskPoolSwitchMap();
        Map<String, Integer> sizeMap = config.getTaskPoolSizeMap();
        Properties prop;
        String poolName;
        int count = 0;
        int size;
        for (TaskType taskType : TaskType.values()) {
            poolName = taskType.name();
            if (!switchMap.containsKey(poolName)) {
                continue;
            }
            if (!switchMap.get(poolName)) {
                continue;
            }
            size = sizeMap.get(poolName);
            if (size == 0) {
                LOG.warn("pool :{} config pool size is 0 ,will change to 1", poolName);
                size = 1;
            }
            prop = DefaultBaseSchedulers.createSimplePrope(size, 1000L);
            boolean createState = createTaskPool(poolName, prop);
            if (createState) {
                startTaskPool(poolName);
            }
            count++;
        }
        LOG.info("pool :{} count: {} started !!!", getAllPoolKey(), count);
        if (config.getSwitchOnTaskType().contains(TaskType.SYSTEM_COPY_CHECK)) {
            SumbitTaskInterface copyJob = createCopySimpleTask(config.getExecuteTaskIntervalTime(),
                                                               TaskType.SYSTEM_COPY_CHECK.name(), service.getServiceId(),
                                                               CopyRecoveryJob.class.getCanonicalName(),
                                                               curatorConfig.getAddresses(), zookeeperPaths.getBaseRoutePath());
            addTask(TaskType.SYSTEM_COPY_CHECK.name(), copyJob);
        }
    }

    /**
     * 概述：生成任务信息
     *
     * @param taskName
     * @param serverId
     * @param clazzName
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private SumbitTaskInterface createCopySimpleTask(long invertalTime, String taskName, String serverId, String clazzName,
                                                     String zkHost, String path) {
        QuartzSimpleInfo task = new QuartzSimpleInfo();
        task.setRunNowFlag(true);
        task.setCycleFlag(true);
        task.setTaskName(taskName);
        task.setTaskGroupName(TaskType.SYSTEM_COPY_CHECK.name());
        task.setRepeateCount(-1);
        task.setInterval(invertalTime);
        Map<String, String> dataMap = JobDataMapConstract.createCOPYDataMap(taskName, serverId, invertalTime, zkHost, path);
        if (dataMap != null && !dataMap.isEmpty()) {
            task.setTaskContent(dataMap);
        }

        task.setClassInstanceName(clazzName);
        return task;
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (taskPoolMap == null || taskPoolMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, BaseSchedulerInterface> entry : taskPoolMap.entrySet()) {
            String name = entry.getKey();
            BaseSchedulerInterface scheduler = entry.getValue();
            if (scheduler.isStart() && !scheduler.isDestory()) {
                scheduler.close(false);
                LOG.info("{} task pool close ", name);
            }
        }
    }
}
