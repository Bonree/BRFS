package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.utils.CreateSystemTask;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSystemTaskWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSystemTaskWorker.class);
    private MetaTaskManagerInterface release;
    private List<TaskType> switchList;
    private Service local;
    private ServiceManager sm;
    private StorageRegionManager snm;

    @Override
    public void run() {
        LOG.info("create system task working");
        //判断是否有恢复任务，有恢复任务则不进行创建
        // 获取开启的任务名称
        if (switchList == null || switchList.isEmpty()) {
            LOG.warn("switch on task is empty !!!");
            return;
        }
        // 获取可用服务
        String groupName = local.getServiceGroup();
        // 2.设置可用服务
        List<String> serverIds = CreateSystemTask.getServerIds(sm, groupName);
        if (serverIds == null || serverIds.isEmpty()) {
            LOG.warn("{} available server list is null", groupName);
            return;
        }
        // 3.获取storageName
        List<StorageRegion> snList = snm.getStorageRegionList();
        if (snList == null || snList.isEmpty()) {
            LOG.info("skip create system task !!! because storageName is null !!!");
            return;
        }
        TaskModel task;
        String taskName;
        TaskTypeModel tmodel;
        Pair<TaskModel, TaskTypeModel> result;
        List<String> srs = TaskStateLifeContral.getSRs(snm);
        for (TaskType taskType : switchList) {
            if (TaskType.SYSTEM_COPY_CHECK.equals(taskType) || TaskType.USER_DELETE.equals(taskType)) {
                continue;
            }
            TaskStateLifeContral.watchSR(release, srs, taskType.name());
            tmodel = release.getTaskTypeInfo(taskType.name());
            if (tmodel == null) {
                tmodel = new TaskTypeModel();
                tmodel.setSwitchFlag(true);
                LOG.warn("taskType{} is switch but metadata is null");
            }
            result = createSystemTask(tmodel, taskType, snList);
            if (result == null) {
                LOG.warn("create sys task is empty {}", taskType.name());
                continue;
            }
            task = result.getFirst();
            taskName = CreateSystemTask.updateTask(release, task, serverIds, taskType);
            if (!BrStringUtils.isEmpty(taskName)) {
                LOG.info("create {} {} task successfull !!!", taskType.name(), taskName);
                release.setTaskTypeModel(taskType.name(), tmodel);
            }
        }
    }

    /***
     * 概述：创建系统任务model
     * @param taskType
     * @param snList
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Pair<TaskModel, TaskTypeModel> createSystemTask(TaskTypeModel tmodel, final TaskType taskType,
                                                           List<StorageRegion> snList) {
        if (snList == null || snList.isEmpty()) {
            return null;
        }
        Map<String, Long> snTimes;
        if (tmodel == null) {
            return null;
        }
        if (!tmodel.isSwitchFlag()) {
            return null;
        }
        snTimes = tmodel.getSnTimes();
        Pair<TaskModel, Map<String, Long>> pair = creatSingleTask(snTimes, snList, taskType);
        if (pair == null) {
            return null;
        }

        snTimes = pair.getSecond();
        if (snTimes != null && !snTimes.isEmpty()) {
            tmodel.putAllSnTimes(snTimes);
        }
        return new Pair<>(pair.getFirst(), tmodel);
    }

    /**
     * 概述：创建单个类型任务
     *
     * @param snTimes
     * @param needSn
     * @param taskType
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Pair<TaskModel, Map<String, Long>> creatSingleTask(final Map<String, Long> snTimes, List<StorageRegion> needSn,
                                                              TaskType taskType) {
        String snName;
        long ctime;
        long startTime;
        long endTime;
        long currentTime = System.currentTimeMillis();
        List<AtomTaskModel> sumAtoms = new ArrayList<>();
        long ttl;
        Map<String, Long> lastSnTimes = new HashMap<>(snTimes);
        long cgratime;
        long granule = 0;
        AtomTaskModel atom;
        for (StorageRegion sn : needSn) {
            granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
            cgratime = currentTime - currentTime % granule;
            snName = sn.getName();
            ctime = sn.getCreateTime();
            // 获取开始时间
            if (snTimes.containsKey(snName)) {
                startTime = snTimes.get(snName);
            } else {
                startTime = ctime - ctime % granule;
            }
            // 获取有效的过期时间
            ttl = getTTL(sn, taskType, 0);
            endTime = startTime + granule;
            LOG.debug("sn : {} ,ttl:{}, taskType,", sn.getName(), ttl, taskType.name());
            // 当ttl小于等于0 的sn 跳过
            if (ttl <= 0) {
                LOG.debug("sn {} don't to create task !!!", snName);
                continue;
            }
            // 当未达到过期的跳过
            if (cgratime - startTime < ttl || cgratime - endTime < ttl) {
                continue;
            }
            // 当前粒度不允许操作
            if (cgratime == startTime) {
                continue;
            }
            atom = AtomTaskModel.getInstance(null, snName, "", sn.getReplicateNum(), startTime, endTime, granule);
            if (atom == null) {
                continue;
            }
            sumAtoms.add(atom);
            lastSnTimes.put(snName, endTime);
        }
        if (sumAtoms.isEmpty()) {
            return null;
        }
        TaskModel task = TaskModel.getInitInstance(taskType);
        task.putAtom(sumAtoms);

        return new Pair<>(task, lastSnTimes);
    }

    /**
     * 概述：首次执行指定任务sn任务创建
     *
     * @param sn
     * @param taskType
     * @param defaultValue
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public long getTTL(StorageRegion sn, TaskType taskType, long defaultValue) {
        if (sn == null) {
            return defaultValue;
        }
        if (taskType == null) {
            return defaultValue;
        }
        if (TaskType.SYSTEM_DELETE.equals(taskType)) {
            return Duration.parse(sn.getDataTtl()).toMillis();
        }
        if (TaskType.SYSTEM_COPY_CHECK.equals(taskType) || TaskType.SYSTEM_CHECK.equals(taskType)) {
            return Duration.parse(sn.getFilePartitionDuration()).toMillis();
        }
        return defaultValue;
    }
}
