package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.files.impl.BRFSTimeFilter;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskStateLifeContral {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateLifeContral.class);

    /**
     * 概述：更新任务状态
     *
     * @param serverId
     * @param taskname
     * @param taskType
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void updateTaskStatusByCompelete(String serverId, String taskname, String taskType,
                                                   TaskResultModel taskResult) {
        if (BrStringUtils.isEmpty(taskname)) {
            LOG.warn("task name is empty !!! {} {} {}", taskType, taskname, serverId);
            return;
        }
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        MetaTaskManagerInterface release = mcf.getTm();
        TaskServerNodeModel contentNodeInfo = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
        if (contentNodeInfo == null) {
            LOG.warn("server task is null !!! {} {} {}", taskType, taskname, serverId);
            contentNodeInfo = new TaskServerNodeModel();
        }
        LOG.debug("TaskMessage complete  contentNodeInfo :{}", JsonUtils.toJsonStringQuietly(contentNodeInfo));
        contentNodeInfo.setResult(taskResult);
        if (BrStringUtils.isEmpty(contentNodeInfo.getTaskStartTime())) {
            contentNodeInfo.setTaskStartTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
        }
        contentNodeInfo.setTaskStopTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
        TaskState status =
            taskResult == null ? TaskState.EXCEPTION : taskResult.isSuccess() ? TaskState.FINISH : TaskState.EXCEPTION;
        contentNodeInfo.setTaskState(status.code());
        release.updateServerTaskContentNode(serverId, taskname, taskType, contentNodeInfo);
        LOG.info("Complete server task :{} - {} - {} - {}", taskType, taskname, serverId,
                 TaskState.valueOf(contentNodeInfo.getTaskState()).name());
        // 更新TaskContent
        List<Pair<String, Integer>> serverStatus = release.getServerStatus(taskType, taskname);
        if (serverStatus == null || serverStatus.isEmpty()) {
            return;
        }
        LOG.debug("complete c List {}", serverStatus);
        int cstat;
        boolean isException = false;
        int finishCount = 0;
        int size = serverStatus.size();
        for (Pair<String, Integer> pair : serverStatus) {
            cstat = pair.getSecond();
            if (TaskState.EXCEPTION.code() == cstat) {
                isException = true;
                finishCount += 1;
            } else if (TaskState.FINISH.code() == cstat) {
                finishCount += 1;
            }
        }
        if (finishCount != size) {
            return;
        }
        TaskModel task = release.getTaskContentNodeInfo(taskType, taskname);
        if (task == null) {
            LOG.warn("task is null !!! {} {} {}", taskType, taskname);
            task = new TaskModel();
            task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
        }
        if (isException) {
            task.setTaskState(TaskState.EXCEPTION.code());
        } else {
            task.setTaskState(TaskState.FINISH.code());
        }
        release.updateTaskContentNode(task, taskType, taskname);
        LOG.info("complete task :{} - {} - {}", taskType, taskname, TaskState.valueOf(task.getTaskState()).name());
        if (TaskType.SYSTEM_CHECK.name().equals(taskType) && isException) {
            TaskModel taskModel = TasksUtils.converyCopyTaskModel(release, taskname);
            if (taskModel == null) {
                LOG.error("[{}]:[{}] task can't recovery !!!", taskType, taskname);
                return;
            }
            String str = TasksUtils.createCopyTask(release, taskname, taskModel);
            if (BrStringUtils.isEmpty(str)) {
                boolean flag = release.setTransferTask(taskType, taskname);
                LOG.info("[{}] task [{}] find error ,transfer task create {}  ", taskType, taskname,
                         flag ? "succefull !!!" : " fail !!!");
            } else {
                LOG.info("[{}]:[{}] find error create copy task [{}] to recovery ", taskType, taskname, str);
            }
        }
    }

    /**
     * 概述：更新任务map的任务状态
     *
     * @param context
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void updateMapTaskMessage(JobExecutionContext context, TaskResultModel result) {
        JobDataMap data = context.getJobDetail().getJobDataMap();
        if (data == null) {
            return;
        }
        // 结果为空不更新批次任务结果
        if (result == null) {
            return;
        }
        boolean isSuccess = result.isSuccess();
        TaskResultModel sumResult;
        String content = null;
        if (data.containsKey(JobDataMapConstract.TASK_RESULT)) {
            content = data.getString(JobDataMapConstract.TASK_RESULT);
        }
        if (!BrStringUtils.isEmpty(content)) {
            sumResult = JsonUtils.toObjectQuietly(content, TaskResultModel.class);
        } else {
            sumResult = new TaskResultModel();
        }
        sumResult.addAll(result.getAtoms());
        sumResult.setSuccess(isSuccess && sumResult.isSuccess());
        String sumContent = JsonUtils.toJsonStringQuietly(sumResult);
        data.put(JobDataMapConstract.TASK_RESULT, sumContent);
    }

    /**
     * 概述：将服务状态修改为RUN
     *
     * @param serverId
     * @param taskname
     * @param taskType
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void updateTaskRunState(String serverId, String taskname, String taskType) {
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        MetaTaskManagerInterface release = mcf.getTm();
        int taskStat = release.queryTaskState(taskname, taskType);
        //修改服务几点状态，若不为RUN则修改为RUN
        TaskServerNodeModel serverNode = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
        if (serverNode == null) {
            serverNode = new TaskServerNodeModel();
        }
        LOG.debug("TaskMessage Run  sTask :{}", JsonUtils.toJsonStringQuietly(serverNode));
        serverNode.setTaskStartTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
        serverNode.setTaskState(TaskState.RUN.code());
        release.updateServerTaskContentNode(serverId, taskname, taskType, serverNode);
        LOG.debug("> run server task :{} - {} - {} - {}", taskType, taskname, serverId,
                  TaskState.valueOf(serverNode.getTaskState()).name());
        //查询任务节点状态，若不为RUN则获取分布式锁，修改为RUN
        if (taskStat != TaskState.RUN.code()) {
            TaskModel task = release.getTaskContentNodeInfo(taskType, taskname);
            if (task == null) {
                task = new TaskModel();
            }
            task.setTaskState(TaskState.RUN.code());
            release.updateTaskContentNode(task, taskType, taskname);
            LOG.debug("run task :{} - {} - {}", taskType, taskname, TaskState.valueOf(task.getTaskState()).name());
        }
    }

    /**
     * 概述：获取当前任务信息
     *
     * @param release
     * @param typeName
     * @param serverId
     * @param limitCount
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Pair<String, TaskModel> getCurrentOperationTask(MetaTaskManagerInterface release, String typeName,
                                                                  String serverId, int limitCount) {
        List<Pair<String, Pair<Integer, Integer>>> needTasks = getServerState(release, typeName, serverId);
        Pair<String, Pair<Integer, Integer>> task = getOperationTask(needTasks, limitCount);
        if (task == null) {
            return null;
        }
        if (BrStringUtils.isEmpty(task.getFirst())) {
            return null;
        }
        TaskModel contentNodeInfo = release.getTaskContentNodeInfo(typeName, task.getFirst());
        if (contentNodeInfo == null) {
            return null;
        }
        //更新异常的次数
        if (task.getSecond().getFirst() == TaskState.EXCEPTION.code()) {
            TaskServerNodeModel server = release.getTaskServerContentNodeInfo(typeName, task.getFirst(), serverId);
            LOG.debug("TaskMessage get  sTask :{}", JsonUtils.toJsonStringQuietly(server));
            server.setRetryCount(server.getRetryCount() + 1);
            release.updateServerTaskContentNode(serverId, task.getFirst(), typeName, server);
        }

        return new Pair<>(task.getFirst(), contentNodeInfo);
    }

    public static TaskModel changeRunTaskModel(final TaskModel message, DiskDaemon diskDaemon) {
        if (message == null) {
            return null;
        }
        // 文件恢复单独不需要处理
        TaskModel changeTask = new TaskModel();
        changeTask.setCreateTime(message.getCreateTime());
        changeTask.setTaskState(changeTask.getTaskState());
        changeTask.setTaskType(message.getTaskType());
        if (TaskType.SYSTEM_COPY_CHECK.code() == changeTask.getTaskType()) {
            changeTask.setAtomList(message.getAtomList());
            return changeTask;
        }
        // 删除任务，校验任务，需要扫目录确定
        List<AtomTaskModel> modelList = message.getAtomList();
        if (modelList == null || modelList.isEmpty()) {
            LOG.warn("task message atom list is empty!!!");
            return null;
        }
        // 循环atom，封装atom
        AtomTaskModel taskModel;
        long startTime;
        long endTime;
        String snName;
        int partNum;
        Map<String, String> map;
        long granule;
        for (AtomTaskModel atom : modelList) {
            startTime = StringUtils.isEmpty(atom.getDataStartTime()) ? 0 :
                TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
            endTime = StringUtils.isEmpty(atom.getDataStopTime()) ? 0 :
                TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
            snName = atom.getStorageName();
            partNum = atom.getPatitionNum();
            granule = atom.getGranule();

            map = new HashMap<>();
            map.put(BRFSPath.STORAGEREGION, snName);
            for (LocalPartitionInfo local : diskDaemon.getPartitions()) {
                List<BRFSPath> dirPaths =
                    BRFSFileUtil.scanBRFSFiles(local.getDataDir(), map, map.size(), new BRFSTimeFilter(startTime, endTime));
                if (dirPaths == null || dirPaths.isEmpty()) {
                    LOG.debug("It's no dir to take task [{}]:[{}]-[{}]", snName,
                              granule <= 0 ? "0" : TimeUtils.timeInterval(startTime, granule),
                              granule <= 0 ? "0" : TimeUtils.timeInterval(endTime, granule));
                    continue;
                }
                List<Long> times = filterRepeatDirs(dirPaths);
                for (Long time : times) {
                    taskModel = AtomTaskModel
                        .getInstance(null, snName, atom.getTaskOperation(), partNum, time, time + atom.getGranule(), 0);
                    changeTask.addAtom(taskModel);
                }
            }
        }
        return changeTask;
    }

    public static List<Long> filterRepeatDirs(List<BRFSPath> files) {
        if (files == null || files.isEmpty()) {
            return new ArrayList<>();
        }
        List<Long> nfiles = new ArrayList<>();
        long time;
        for (BRFSPath file : files) {
            time = file.toTimeMile();
            if (!nfiles.contains(time)) {
                nfiles.add(time);
            }
        }
        return nfiles;
    }

    /**
     * 概述：将任务分批
     *
     * @param message
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    @Deprecated
    public static TaskModel changeRunTaskModel(final TaskModel message) {
        if (message == null) {
            return null;
        }

        TaskModel changeTask = new TaskModel();
        changeTask.setCreateTime(message.getCreateTime());
        changeTask.setTaskState(changeTask.getTaskState());
        changeTask.setTaskType(message.getTaskType());
        if (TaskType.SYSTEM_COPY_CHECK.code() == changeTask.getTaskType()) {
            changeTask.setAtomList(message.getAtomList());
            return changeTask;
        }
        List<AtomTaskModel> atoms = message.getAtomList();
        AtomTaskModel atom;
        if (atoms == null || atoms.isEmpty()) {
            return changeTask;
        }
        long startTime;
        long endTime;
        String snName;
        String operation;
        long granule;
        for (AtomTaskModel taskModel : atoms) {
            startTime = TimeUtils.getMiles(taskModel.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
            endTime = TimeUtils.getMiles(taskModel.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
            snName = taskModel.getStorageName();
            operation = taskModel.getTaskOperation();
            granule = taskModel.getGranule();
            for (long start = startTime; start < endTime; start += granule) {
                if (start + granule > endTime) {
                    continue;
                }
                atom =
                    AtomTaskModel.getInstance(null,
                                              snName,
                                              operation,
                                              taskModel.getPatitionNum(),
                                              start,
                                              start + granule,
                                              granule);
                changeTask.addAtom(atom);
            }
        }
        return changeTask;
    }

    /**
     * 概述：获取指定任务的队列
     *
     * @param release
     * @param typeName
     * @param serverId
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<Pair<String, Pair<Integer, Integer>>> getServerState(MetaTaskManagerInterface release, String typeName,
                                                                            String serverId) {
        List<String> taskNames = release.getTaskList(typeName);
        List<Pair<String, Pair<Integer, Integer>>> taskStatuss = new ArrayList<>();
        if (taskNames == null || taskNames.isEmpty()) {
            return taskStatuss;
        }
        Pair<String, Pair<Integer, Integer>> taskStatus;
        Pair<Integer, Integer> codeAndCount;
        TaskServerNodeModel server;
        for (String taskName : taskNames) {
            server = release.getTaskServerContentNodeInfo(typeName, taskName, serverId);
            if (server == null) {
                continue;
            }
            if (server.getTaskState() == TaskState.FINISH.code()) {
                continue;
            }
            codeAndCount = new Pair<>(server.getTaskState(), server.getRetryCount());
            taskStatus = new Pair<>(taskName, codeAndCount);
            taskStatuss.add(taskStatus);
        }
        return taskStatuss;
    }

    /**
     * 概述：获取当前执行的任务
     *
     * @param tasks
     * @param limtCount
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Pair<String, Pair<Integer, Integer>> getOperationTask(List<Pair<String, Pair<Integer, Integer>>> tasks,
                                                                        int limtCount) {
        if (tasks == null || tasks.isEmpty()) {
            return null;
        }
        List<Pair<String, Pair<Integer, Integer>>> etasks = new ArrayList<>();
        Pair<Integer, Integer> codeAndCount;
        for (Pair<String, Pair<Integer, Integer>> task : tasks) {
            codeAndCount = task.getSecond();
            if (codeAndCount == null) {
                continue;
            }
            if (codeAndCount.getFirst() == TaskState.FINISH.code() || codeAndCount.getFirst() == TaskState.RUN.code()
                || codeAndCount.getFirst() == TaskState.RERUN.code()) {
                continue;
            }
            if (codeAndCount.getFirst() == TaskState.INIT.code()) {
                return task;
            } else if (codeAndCount.getFirst() == TaskState.EXCEPTION.code() && limtCount > 0) {
                if (codeAndCount.getSecond() > limtCount) {
                    etasks.add(task);
                } else {
                    return task;
                }
            }
        }
        if (etasks.isEmpty()) {
            return null;
        }
        etasks.sort((o1, o2) -> {
            if (o1 == null || o1.getSecond() == null) {
                return -1;
            }
            if (o2 == null || o2.getSecond() == null) {
                return 1;
            }
            if (o1.getSecond().getSecond() > o2.getSecond().getSecond()) {
                return -1;
            } else if (o1.getSecond().getSecond().equals(o2.getSecond().getSecond())) {
                return 0;
            } else {
                return 1;
            }

        });
        return etasks.get(etasks.size() - 1);
    }

    /**
     * 概述：去掉已删除的sn
     *
     * @param release
     * @param srs
     * @param taskType
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void watchSR(MetaTaskManagerInterface release, List<String> srs, String taskType) {
        TaskTypeModel typeModel = release.getTaskTypeInfo(taskType);
        if (typeModel == null) {
            LOG.warn("tasktype : {} meta data loss !!!", taskType);
            return;
        }
        Map<String, Long> snMap = typeModel.getSnTimes();
        if (snMap == null || snMap.isEmpty()) {
            return;
        }
        List<String> deleteSRs = new ArrayList<>();
        for (String srName : snMap.keySet()) {
            if (srs.contains(srName)) {
                continue;
            }
            deleteSRs.add(srName);
        }
        if (deleteSRs.isEmpty()) {
            return;
        }
        for (String str : deleteSRs) {
            typeModel.removesnTime(str);
        }
        release.setTaskTypeModel(taskType, typeModel);
    }

    /***
     * 概述：转换
     * @param srm
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<String> getSRs(StorageRegionManager srm) {
        List<StorageRegion> srList = srm.getStorageRegionList();
        List<String> srs = new ArrayList<>();
        if (srList == null || srList.isEmpty()) {
            return srs;
        }
        String srName;
        for (StorageRegion sr : srList) {
            srName = sr.getName();
            if (BrStringUtils.isEmpty(srName)) {
                continue;
            }
            srs.add(srName);
        }
        return srs;
    }
}
