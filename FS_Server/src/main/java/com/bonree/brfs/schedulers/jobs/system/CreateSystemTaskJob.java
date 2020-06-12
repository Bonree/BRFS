package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.CreateSystemTask;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSystemTaskJob extends QuartzOperationStateTask {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSystemTaskJob.class);

    @Override
    public void interrupt() {
    }

    @Override
    public void operation(JobExecutionContext context) {
        LOG.info("create system task working");
        //判断是否有恢复任务，有恢复任务则不进行创建
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        MetaTaskManagerInterface release = mcf.getTm();
        // 获取开启的任务名称
        List<TaskType> switchList = mcf.getTaskOn();
        if (switchList == null || switchList.isEmpty()) {
            LOG.warn("switch on task is empty !!!");
            return;
        }
        // 获取可用服务
        String groupName = mcf.getGroupName();
        ServiceManager sm = mcf.getSm();
        // 2.设置可用服务
        List<String> serverIds = CreateSystemTask.getServerIds(sm, groupName);
        if (serverIds == null || serverIds.isEmpty()) {
            LOG.warn("{} available server list is null", groupName);
            return;
        }
        // 3.获取storageName
        StorageRegionManager snm = mcf.getSnm();
        List<StorageRegion> snList = snm.getStorageRegionList();
        if (snList == null || snList.isEmpty()) {
            LOG.info("skip create system task !!! because storageName is null !!!");
            return;
        }
        TaskModel task;
        String taskName;
        TaskTypeModel tmodel = null;
        Pair<TaskModel, TaskTypeModel> result;
        List<String> srs = TaskStateLifeContral.getSRs(snm);
        for (TaskType taskType : switchList) {
            if (TaskType.SYSTEM_COPY_CHECK.equals(taskType)
                || TaskType.USER_DELETE.equals(taskType)) {
                continue;
            }
            if (TaskType.VIRTUAL_ID_RECOVERY.equals(taskType)) {
                task =
                    createVirtualTask(release, mcf.getSim().getVirtualServerID(), mcf.getSnm(), mcf.getSm(), mcf.getTaskMonitor(),
                                      mcf.getGroupName());
            } else {
                TaskStateLifeContral.watchSR(release, srs, taskType.name());
                tmodel = release.getTaskTypeInfo(taskType.name());
                if (tmodel == null) {
                    tmodel = new TaskTypeModel();
                    tmodel.setSwitchFlag(true);
                    LOG.warn("taskType{} is switch but metadata is null");
                }
                result = CreateSystemTask.createSystemTask(tmodel, taskType, snList);

                if (result == null) {
                    LOG.debug("create sys task is empty {}", taskType.name());
                    continue;
                }
                task = result.getFirst();
            }
            if (task != null) {
                taskName = CreateSystemTask.updateTask(release, task, serverIds, taskType);
                if (!BrStringUtils.isEmpty(taskName) && !TaskType.VIRTUAL_ID_RECOVERY.equals(taskType)) {
                    release.setTaskTypeModel(taskType.name(), tmodel);
                }
                LOG.info("create {} {} task successfull !!!", taskType.name(), taskName);
            }
        }
    }

    public TaskModel createVirtualTask(MetaTaskManagerInterface release, VirtualServerID idsManager,
                                       StorageRegionManager regionManager, ServiceManager serviceManager,
                                       RebalanceTaskMonitor taskMonitor, String group) {
        if (taskMonitor.isExecute()) {
            return null;
        }
        List<StorageRegion> regions = regionManager.getStorageRegionList();
        List<Service> services = serviceManager.getServiceListByGroup(group);
        if (services == null || services.isEmpty()) {
            return null;
        }
        List<String> servers = services.stream().map(Service::getServiceId).collect(Collectors.toList());
        if (servers == null || servers.isEmpty()) {
            LOG.info("servers {} [{}]", servers, services);
            return null;
        }
        if (regions == null || regions.isEmpty()) {
            return null;
        }
        Map<StorageRegion, List<String>> virtualMap = new HashMap<>();
        for (StorageRegion region : regions) {
            if (taskMonitor.isExecute()) {
                return null;
            }
            List<String> virtuals = idsManager.listVirtualIds(region.getId());
            if (virtuals == null || virtuals.isEmpty()) {
                continue;
            }
            List<String> recoverys = new ArrayList<>();
            for (String virtual : virtuals) {
                List<String> tmps = idsManager.listFirstServer(region.getId(), virtual);
                if (tmps.isEmpty()) {
                    continue;
                }
                boolean add = false;
                for (String first : servers) {
                    if (!tmps.contains(first)) {
                        add = true;
                        break;
                    }
                }
                if (add) {
                    recoverys.add(virtual);
                }
            }
            virtualMap.put(region, virtuals);
        }
        if (virtualMap.isEmpty()) {
            LOG.info("no virtual id to recovery");
            return null;
        }
        List<String> tasks = release.getTaskList(TaskType.VIRTUAL_ID_RECOVERY.name());
        if (tasks != null) {
            for (String x : tasks) {
                if (taskMonitor.isExecute()) {
                    return null;
                }
                TaskModel model = release.getTaskContentNodeInfo(TaskType.VIRTUAL_ID_RECOVERY.name(), x);
                if (model != null && model.getTaskState() != TaskState.FINISH.code()) {
                    LOG.info("there is  virtual id  recovery task");
                    return null;
                }
            }
        }
        long time = System.currentTimeMillis();
        String creatTime = TimeUtils.formatTimeStamp(time, TimeUtils.TIME_MILES_FORMATE);
        TaskModel task = new TaskModel();
        task.setTaskState(TaskState.INIT.code());
        task.setCreateTime(creatTime);
        task.setTaskType(TaskType.VIRTUAL_ID_RECOVERY.code());
        List<AtomTaskModel> atoms = new ArrayList<>();
        for (Map.Entry<StorageRegion, List<String>> entry : virtualMap.entrySet()) {
            if (taskMonitor.isExecute()) {
                return null;
            }
            StorageRegion region1 = entry.getKey();
            List<String> tmpVirtuals = entry.getValue();
            for (String virtual : tmpVirtuals) {
                AtomTaskModel atom = new AtomTaskModel();
                atom.setStorageName(region1.getName());
                atom.setTaskOperation(virtual);
                atoms.add(atom);
            }
        }
        if (atoms.isEmpty()) {
            return null;
        }
        task.setAtomList(atoms);
        return task;

    }
}
