package com.bonree.brfs.schedulers;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.disknode.TaskConfig;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.system.CheckCycleJob;
import com.bonree.brfs.schedulers.jobs.system.CopyCheckJob;
import com.bonree.brfs.schedulers.jobs.system.CreateSystemTaskJob;
import com.bonree.brfs.schedulers.jobs.system.ManagerMetaTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzCronInfo;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年5月10日 下午5:16:45
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 选取任务管理leader
 *****************************************************************************
 */
public class MetaTaskLeaderManager implements LeaderLatchListener {
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
        .setNameFormat("TaskLeader")
        .setDaemon(true)
        .build();
    private static final Logger LOG = LoggerFactory.getLogger(MetaTaskLeaderManager.class);
    private static final String META_TASK_MANAGER = "META_TASK_MANAGER";
    private static final String COPY_CYCLE_POOL = "COPY_CYCLE_POOL";
    private MetaWorker worker = null;
    private ExecutorService pool = null;

    @Inject
    public MetaTaskLeaderManager(SchedulerManagerInterface manager, MetaTaskManagerInterface release, TaskConfig config) {
        this.worker = new MetaWorker(manager, release, config);
    }

    @Override
    public void isLeader() {
        pool = Executors.newSingleThreadExecutor(THREAD_FACTORY);
        pool.submit(this.worker);
    }

    @Override
    public void notLeader() {
        try {
            this.worker.close();
            if (this.pool != null) {
                this.pool.shutdownNow();
            }
        } catch (IOException e) {
            LOG.error("close meta manager happen error", e);
        }
        LOG.info("loss the leader !!!");
    }

    private class MetaWorker implements Runnable, Closeable {
        private SchedulerManagerInterface manager;
        private TaskConfig config;
        private MetaTaskManagerInterface release;

        public MetaWorker(SchedulerManagerInterface manager, MetaTaskManagerInterface release, TaskConfig config) {
            this.manager = manager;
            this.release = release;
            this.config = config;
        }

        @Override
        public void run() {
            try {
                LOG.info("==========================LEADER=================================");
                // 若接口为空则返回空
                if (manager == null) {
                    LOG.warn("SchedulerManagerInterface is null, Loss biggerst !!!");
                    return;
                }
                Properties prop = DefaultBaseSchedulers.createSimplePrope(3, 1000L);
                boolean createFlag;
                createFlag = manager.createTaskPool(META_TASK_MANAGER, prop);
                // 若创建不成功则返回
                if (!createFlag) {
                    LOG.warn("create task manager server fail !!!!");
                    return;
                }
                boolean startTaskPool = manager.startTaskPool(META_TASK_MANAGER);
                if (!startTaskPool) {
                    LOG.info("Follower will quiting  !!!");
                    return;
                }
                LOG.info("Leader create task manager server success !!!");
                checkSwitchTask(config, release);
                if (!config.getTaskTypeSwitch().isEmpty()) {
                    sumbitTask(manager, config);
                    createCheckCyclePool(manager, config);
                }
                LOG.info("==========================LEADER FINISH=================================");
            } catch (Exception e) {
                LOG.error("create meta task manager happen error ", e);
            }
        }

        /**
         * 同步zk任务的配置信息
         *
         * @param taskConfig
         * @param release
         */
        public void checkSwitchTask(TaskConfig taskConfig, MetaTaskManagerInterface release) {
            Collection<TaskType> taskTypes = taskConfig.getTaskTypeSwitch();
            for (TaskType taskType : TaskType.getDefaultTaskType()) {
                TaskTypeModel type = release.getTaskTypeInfo(taskType.name());
                if (type == null) {
                    if (taskTypes.contains(taskType)) {
                        type = new TaskTypeModel();
                        release.setTaskTypeModel(taskType.name(), type);
                    }
                    continue;
                }
                if (type.isSwitchFlag() != taskTypes.contains(taskType)) {
                    type.setSwitchFlag(taskTypes.contains(taskType));
                    release.setTaskTypeModel(taskType.name(), type);
                }

            }
        }

        /**
         * 概述：提交任务
         *
         * @throws ParamsErrorException
         * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
         */
        public void sumbitTask(SchedulerManagerInterface manager, TaskConfig taskConfig) throws ParamsErrorException {
            Collection<TaskType> taskTypes = taskConfig.getTaskTypeSwitch();
            long createIntervalMil = TimeUnit.SECONDS.toMillis(taskConfig.getCommonCreateIntervalSecond());
            SumbitTaskInterface createJob = QuartzSimpleInfo.createCycleTaskInfo("CREATE_SYSTEM_TASK",
                                                                                 createIntervalMil,
                                                                                 60000,
                                                                                 new HashMap<>(), CreateSystemTaskJob.class);
            SumbitTaskInterface metaJob = QuartzSimpleInfo.createCycleTaskInfo("META_MANAGER_TASK",
                                                                               createIntervalMil,
                                                                               60000,
                                                                               new HashMap<>(),
                                                                               ManagerMetaTaskJob.class);

            boolean isSuccess = false;
            isSuccess = manager.addTask(META_TASK_MANAGER, createJob);
            LOG.info("sumbit create Job {} ", isSuccess ? " Sucess" : "Fail");
            isSuccess = manager.addTask(META_TASK_MANAGER, metaJob);
            LOG.info("sumbit meta Job {} ", isSuccess ? " Sucess" : "Fail");
            if (taskTypes.contains(TaskType.SYSTEM_COPY_CHECK)) {
                SumbitTaskInterface checkJob = QuartzSimpleInfo.createCycleTaskInfo("COPY_CHECK_TASK",
                                                                                    createIntervalMil,
                                                                                    60000,
                                                                                    new HashMap<>(), CopyCheckJob.class);
                isSuccess = manager.addTask(META_TASK_MANAGER, checkJob);
                LOG.info("sumbit Create Check Job {} ", isSuccess ? " Sucess" : "Fail");
            }

        }

        /**
         * 创建周期检查任务
         *
         * @param manager
         * @param taskConfig
         *
         * @throws ParamsErrorException
         */
        public void createCheckCyclePool(SchedulerManagerInterface manager, TaskConfig taskConfig)
            throws Exception {
            if (!taskConfig.getTaskTypeSwitch().contains(TaskType.SYSTEM_COPY_CHECK)) {
                return;
            }
            Properties prop = DefaultBaseSchedulers.createSimplePrope(1, 1000L);

            boolean createFlag = manager.createTaskPool(COPY_CYCLE_POOL, prop);

            if (!createFlag) {
                LOG.warn("create check pool fail !!!!");
                return;
            }
            boolean startTaskPool = manager.startTaskPool(COPY_CYCLE_POOL);
            if (!startTaskPool) {
                LOG.info("Follower will quiting  !!!");
                return;
            }
            SumbitTaskInterface sumbit = QuartzCronInfo
                .getInstance("CYCLE_CHECK_JOB", "CYCLE_CHECK_JOB", taskConfig.getCronStr(), new HashMap<>(),
                             CheckCycleJob.class);
            startTaskPool = manager.addTask("COPY_CYCLE_POOL", sumbit);
            LOG.info("sumbit Cycle task :{}", Boolean.valueOf(startTaskPool));
        }

        @Override
        public void close() throws IOException {
            try {
                // 若接口为空则返回空
                if (manager == null) {
                    LOG.warn("SchedulerManagerInterface is null, No to do");
                    return;
                }
                manager.destoryTaskPool(META_TASK_MANAGER, false);
                manager.destoryTaskPool(COPY_CYCLE_POOL, false);
            } catch (ParamsErrorException e) {
                LOG.error("close meta task happen error ", e);
            }
        }
    }

}
