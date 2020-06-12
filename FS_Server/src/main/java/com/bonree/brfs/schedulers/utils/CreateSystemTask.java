package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年6月4日 下午9:01:11
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:
 *****************************************************************************
 */
public class CreateSystemTask {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSystemTask.class);

    /***
     * 概述：创建系统任务model
     * @param taskType
     * @param snList
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Pair<TaskModel, TaskTypeModel> createSystemTask(TaskTypeModel tmodel, final TaskType taskType,
                                                                  final List<StorageRegion> snList) {
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
    public static Pair<TaskModel, Map<String, Long>> creatTaskWithFiles(final Map<String, Long> snTimes,
                                                                        final Map<String, List<String>> snFiles,
                                                                        List<StorageRegion> needSn, TaskType taskType,
                                                                        String taskOperation, long globalTTL) {
        if (needSn == null || snTimes == null) {
            return null;
        }
        String snName;
        long startTime;
        long endTime;
        long currentTime = System.currentTimeMillis();
        List<AtomTaskModel> sumAtoms = new ArrayList<>();
        long ttl;
        Map<String, Long> lastSnTimes = new HashMap<>(snTimes);
        List<String> files = null;
        AtomTaskModel atom;
        long cgratime;
        long granule = 0;
        for (StorageRegion sn : needSn) {
            granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
            cgratime = currentTime - (currentTime % granule);
            snName = sn.getName();
            // 获取开始时间
            if (!snTimes.containsKey(snName)) {
                continue;
            }
            startTime = snTimes.get(snName);
            // 获取有效的过期时间
            ttl = getTTL(sn, taskType, globalTTL);
            endTime = startTime + granule;
            // 当ttl小于等于0 的sn 跳过
            if (ttl < 0) {
                LOG.debug("sn {} don't to create task !!!", snName);
                continue;
            }
            // 当未达到过期的跳过
            if (cgratime - startTime < ttl || cgratime - endTime < ttl) {
                LOG.debug("it's not time to check {}", snName);
                continue;
            }
            // 当前粒度不允许操作
            if (cgratime == startTime) {
                LOG.warn("current time is forbid to check !!!");
                continue;
            }
            // 当无文件不操作
            if (snFiles != null) {
                files = snFiles.get(snName);
            }
            if (files != null && !files.isEmpty()) {
                atom = AtomTaskModel.getInstance(files, snName, taskOperation, sn.getReplicateNum(), startTime, endTime, granule);
                sumAtoms.add(atom);
            }
            lastSnTimes.put(snName, endTime);
        }
        if (sumAtoms == null || sumAtoms.isEmpty()) {
            return new Pair<>(null, lastSnTimes);
        }
        TaskModel task = TaskModel.getInitInstance(taskType);
        task.putAtom(sumAtoms);
        return new Pair<>(task, lastSnTimes);
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
    public static Pair<TaskModel, Map<String, Long>> creatSingleTask(final Map<String, Long> snTimes, List<StorageRegion> needSn,
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
    public static long getTTL(StorageRegion sn, TaskType taskType, long defaultValue) {
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

    /**
     * 概述：将任务信息创建到zk
     *
     * @param release
     * @param task
     * @param serverIds
     * @param taskType
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static String updateTask(MetaTaskManagerInterface release, TaskModel task, List<String> serverIds, TaskType taskType) {
        if (task == null) {
            LOG.warn(" task create is null skip ");
            return null;
        }
        String taskName = release.updateTaskContentNode(task, taskType.name(), null);
        if (taskName == null) {
            LOG.warn("create task error : taskName is empty");
            return null;
        }
        TaskServerNodeModel nodeModel = TaskServerNodeModel.getInitInstance();
        for (String serviceId : serverIds) {
            release.updateServerTaskContentNode(serviceId, taskName, taskType.name(), nodeModel);
        }
        return taskName;
    }

    /***
     * 概述：获取存活的serverid
     * @param sm
     * @param groupName
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<String> getServerIds(ServiceManager sm, String groupName) {
        List<Service> services = sm.getServiceListByGroup(groupName);

        return getServerIds(services);
    }

    public static List<String> getServerIds(List<Service> services) {
        List<String> sids = new ArrayList<>();
        if (services == null || services.isEmpty()) {
            return sids;
        }
        String sid;
        for (Service server : services) {
            sid = server.getServiceId();
            if (BrStringUtils.isEmpty(sid)) {
                continue;
            }
            sids.add(sid);
        }
        return sids;
    }
}
