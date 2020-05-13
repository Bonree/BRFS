package com.bonree.brfs.tasks.manager;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.system.OperationTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务执行器
 */
@ManageLifecycle
public class TaskOpertionManager implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(TaskOpertionManager.class);
    public static final String TASK_OPERATION_MANAGER = "TASK_OPERATION_MANAGER";
    private ManagerContralFactory mcf;
    private ResourceTaskConfig confg;

    @Inject
    public TaskOpertionManager(ManagerContralFactory mcf, ResourceTaskConfig confg) {
        this.mcf = mcf;
        this.confg = confg;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if (!confg.isTaskFrameWorkSwitch()) {
            LOG.info("task framework switch close");
            return;
        }
        List<TaskType> switchList = confg.getSwitchOnTaskType();
        SchedulerManagerInterface manager = mcf.getStm();
        MetaTaskManagerInterface release = mcf.getTm();
        String serverId = mcf.getServerId();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Properties prop = DefaultBaseSchedulers.createSimplePrope(3, 1000);
                    boolean createFlag = manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
                    if (!createFlag) {
                        LOG.error("create task operation error !!!");
                        throw new NullPointerException("create task operation error !!!");
                    }
                    boolean startTaskPool = manager.startTaskPool(TASK_OPERATION_MANAGER);
                    if (!startTaskPool) {
                        LOG.error("create task operation error !!!");
                        throw new NullPointerException("start task operation error !!!");
                    }
                    Map<String, String> switchMap = null;
                    // 将任务信息不完全的任务补充完整
                    LOG.info("========================================================================================");
                    switchMap = recoveryTask(switchList, release, serverId);
                    LOG.info("========================================================================================");
                    SumbitTaskInterface task = QuartzSimpleInfo
                        .createCycleTaskInfo(TASK_OPERATION_MANAGER, confg.getExecuteTaskIntervalTime(), 60000, switchMap,
                                             OperationTaskJob.class);
                    boolean sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
                    if (sumbitFlag) {
                        LOG.info("operation task sumbit complete !!!");
                    }
                } catch (ParamsErrorException e) {
                    throw new RuntimeException("create task opertion manager happen error ", e);
                }
            }
        }).start();
    }

    /**
     * 概述：修复任务状态
     *
     * @param swtichList
     * @param release
     * @param serverId
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private static Map<String, String> recoveryTask(List<TaskType> swtichList, MetaTaskManagerInterface release,
                                                    String serverId) {
        Map<String, String> swtichMap = new HashMap<>();
        if (swtichList == null || swtichList.isEmpty()) {
            return swtichMap;
        }
        String typeName;
        String currentTask;
        for (TaskType taskType : swtichList) {
            typeName = taskType.name();
            currentTask = release.getLastSuccessTaskIndex(typeName, serverId);
            if (BrStringUtils.isEmpty(currentTask)) {
                currentTask = release.getFirstServerTask(typeName, serverId);
            }
            if (BrStringUtils.isEmpty(currentTask)) {
                currentTask = release.getFirstTaskName(typeName);
            }
            if (BrStringUtils.isEmpty(currentTask)) {
                LOG.info("{} task queue is empty", currentTask);
                continue;
            }
            // 修复任务
            recoveryTask(release, typeName, currentTask, serverId);
            if (BrStringUtils.isEmpty(currentTask)) {
                continue;
            }
            swtichMap.put(typeName, currentTask);
        }
        return swtichMap;
    }

    /**
     * 概述：将因服务挂掉而错失的任务重建
     *
     * @param release
     * @param taskType
     * @param currentTask
     * @param serverId
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private static void recoveryTask(MetaTaskManagerInterface release, String taskType, String currentTask, String serverId) {
        List<String> tasks = release.getTaskList(taskType);
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        int index = tasks.indexOf(currentTask);
        if (index < 0) {
            index = 0;
        }
        int size = tasks.size();
        String taskName;
        List<String> strings;
        TaskServerNodeModel serverNode = TaskServerNodeModel.getInitInstance();
        TaskServerNodeModel change;
        for (int i = index; i < size; i++) {
            taskName = tasks.get(i);
            if (BrStringUtils.isEmpty(taskName)) {
                continue;
            }
            strings = release.getTaskServerList(taskType, taskName);
            if (strings == null || strings.isEmpty() || !strings.contains(serverId)) {
                release.updateServerTaskContentNode(serverId, taskName, taskType, serverNode);
                int stat = release.queryTaskState(taskName, taskType);
                if (TaskState.FINISH.code() == stat) {
                    release.changeTaskContentNodeState(taskName, taskType, TaskState.RERUN.code());
                }
                LOG.info("Recover {} task's {} serverId  {} ", taskType, taskName, serverId);
                continue;
            }
            change = release.getTaskServerContentNodeInfo(taskType, taskName, serverId);
            if (change == null) {
                change = TaskServerNodeModel.getInitInstance();
            }
            if (change.getTaskState() == TaskState.RUN.code() || change.getTaskState() == TaskState.RERUN.code()) {
                change.setTaskState(TaskState.INIT.code());
                release.updateServerTaskContentNode(serverId, taskName, taskType, change);
            }
        }
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (!confg.isTaskFrameWorkSwitch()) {
            LOG.info("task framework switch close");
            return;
        }
    }
}
