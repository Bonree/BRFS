package com.bonree.brfs.schedulers.task.manager.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import javax.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReleaseTask implements MetaTaskManagerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultReleaseTask.class);
    private static final String QUEUE = "queue";
    private static final String TRANSFER = "transfer";
    private static final String HISTORY = "history";
    private String taskRootPath = null;
    private CuratorFramework zkclient = null;
    private String taskQueue = null;
    private String taskTransfer = null;
    private String taskHistory = null;

    @Inject
    public DefaultReleaseTask(CuratorFramework client, ZookeeperPaths zkPath) {
        this(client, zkPath.getBaseTaskPath());
    }

    public DefaultReleaseTask(CuratorFramework client, String taskRootPath) {
        this.zkclient = client;
        this.taskRootPath = taskRootPath;
        Preconditions.checkNotNull(this.taskRootPath, "task root path is empty");
        this.taskQueue = ZKPaths.makePath(this.taskRootPath, QUEUE);
        this.taskTransfer = ZKPaths.makePath(this.taskRootPath, TRANSFER);
        this.taskHistory = ZKPaths.makePath(this.taskRootPath, HISTORY);
    }

    @Override
    public String updateTaskContentNode(TaskModel data, String taskType, String taskName) {
        return updateTaskContentNode(data, taskType, taskName, this.taskQueue);
    }

    private String updateTaskContentNode(TaskModel data, String taskType, String taskName, String taskQueuePath) {
        String pathNode = null;
        try {
            Preconditions.checkNotNull(data, "task content is empty");
            byte[] datas = JsonUtils.toJsonBytes(data);
            Preconditions.checkNotNull(datas, "task content convert is empty");
            Preconditions.checkNotNull(taskType, "task type is empty");

            TaskType current = TaskType.valueOf(taskType);
            int taskTypeIndex = current == null ? 0 : current.code();

            StringBuilder pathBuilder = new StringBuilder();
            pathBuilder.append(taskQueuePath).append("/").append(taskType).append("/");
            if (BrStringUtils.isEmpty(taskName)) {
                pathBuilder.append(taskTypeIndex);
            } else {
                pathBuilder.append(taskName);
            }
            String taskPath = pathBuilder.toString();
            if (!BrStringUtils.isEmpty(taskName) && zkclient.checkExists().forPath(taskPath) != null) {
                zkclient.setData().forPath(taskPath, datas);
                return taskName;
            }
            pathNode = zkclient.create()
                               .creatingParentsIfNeeded()
                               .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                               .forPath(taskPath, datas);

            String[] nodes = BrStringUtils.getSplit(pathNode, "/");
            if (nodes != null && nodes.length != 0) {
                return nodes[nodes.length - 1];
            }
        } catch (Exception e) {
            LOG.error("update task error {}", e);
        }
        return pathNode;
    }

    @Override
    public int queryTaskState(String taskName, String taskType) {
        return queryTaskState(taskName, taskType, this.taskQueue);
    }

    private int queryTaskState(String taskName, String taskType, String taskQueuePath) {
        try {
            Preconditions.checkNotNull(taskQueuePath, "queue is empty !!");
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(taskName, "taskName is empty !!");
            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName);
            TaskModel tmp = getObject(path, TaskModel.class);
            return tmp.getTaskState();
        } catch (Exception e) {
            LOG.error("query task status error {}", e);
        }
        return -1;
    }

    @Override
    public boolean updateServerTaskContentNode(String serverId, String taskName, String taskType, TaskServerNodeModel data) {
        return updateServerTaskContentNode(serverId, taskName, taskType, data, this.taskQueue);
    }

    /**
     * 更新服务节点任务信息
     *
     * @param serverId
     * @param taskName
     * @param taskType
     * @param data
     * @param taskQueuePath
     *
     * @return
     */
    private boolean updateServerTaskContentNode(String serverId, String taskName, String taskType, TaskServerNodeModel data,
                                                String taskQueuePath) {

        try {
            Preconditions.checkNotNull(taskQueuePath, "queue is empty !!");
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(taskName, "taskName is empty !!");
            Preconditions.checkNotNull(serverId, "serverID is empty !!");
            Preconditions.checkNotNull(data, "task content is empty !!");

            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName, serverId);
            setPersistentObject(path, data);
            return true;
        } catch (Exception e) {
            LOG.error("update server task status error {}", e);
        }
        return false;
    }

    @Override
    public boolean changeTaskContentNodeState(String taskName, String taskType, int taskState) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(taskName, "taskName is empty !!");
            TaskModel tmp = getTaskContentNodeInfo(taskType, taskName);
            if (tmp == null) {
                return false;
            }
            tmp.setTaskState(taskState);
            updateTaskContentNode(tmp, taskType, taskName);
            return true;
        } catch (Exception e) {
            LOG.error("update task content node error {}", e);
        }
        return false;
    }

    @Override
    public String getLastSuccessTaskIndex(String taskType, String serverId) {
        try {
            List<String> taskInfos = getTaskList(taskType, this.taskQueue);
            if (taskInfos == null || taskInfos.isEmpty()) {
                return null;
            }
            int maxIndex = taskInfos.size() - 1;
            for (int i = maxIndex; i >= 0; i--) {
                String taskName = taskInfos.get(i);
                String taskPath = ZKPaths.makePath(this.taskQueue, taskType, taskName, serverId);
                TaskServerNodeModel tmpR = getObject(taskPath, TaskServerNodeModel.class);
                if (tmpR == null) {
                    continue;
                }
                if (tmpR.getTaskState() != TaskState.FINISH.code()) {
                    continue;
                }
                return taskInfos.get(i);
            }
        } catch (Exception e) {
            LOG.error("{}", e);
        }
        return null;
    }

    private List<String> collectHistoryFailTasks(String taskType, String serverId, String queue) {
        List<String> historyQueue = getTaskList(taskType, queue);
        if (historyQueue == null || historyQueue.isEmpty()) {
            return ImmutableList.of();
        }
        int index = searchFirstTask(historyQueue, taskType, serverId, queue);
        if (index < 0) {
            return ImmutableList.of();
        }
        return historyQueue.subList(index, historyQueue.size());
    }

    private int searchFirstTask(List<String> tasks, String taskType, String serverId, String queue) {
        int low = 0;
        int hight = tasks.size() - 1;
        while (low <= hight) {
            int mid = low + (hight - low) / 2;
            String taskName = tasks.get(mid);
            if (isException(queue, taskType, taskName, serverId)) {
                hight = mid - 1;
                if (hight >= 0 && hight < tasks.size()) {
                    taskName = tasks.get(hight);
                    boolean exception = isException(queue, taskType, taskName, serverId);
                    if (!exception) {
                        return mid;
                    } else if (hight == 0 && exception) {
                        return hight;
                    }
                } else {
                    return mid;
                }
            } else {
                low = mid + 1;
                if (low >= 0 && low < tasks.size()) {
                    taskName = tasks.get(low);
                    if (isException(queue, taskType, taskName, serverId)) {
                        return low;
                    }
                }
            }

        }
        return -1;
    }

    private boolean isException(String queue, String taskType, String checkName, String serverId) {
        try {
            Preconditions.checkNotNull(queue, "queue is empty !!");
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(checkName, "taskName is empty !!");
            Preconditions.checkNotNull(serverId, "serverId path is empty !!");
            String path = ZKPaths.makePath(queue, taskType, checkName, serverId);
            return TaskState.EXCEPTION.code() == getObject(path, TaskServerNodeModel.class).getTaskState();
        } catch (Exception e) {
            LOG.error("check task stat happen error ", e);
        }
        return false;

    }

    @Override
    public String getFirstServerTask(String taskType, String serverId) {
        List<String> taskInfos = getTaskList(taskType);
        if (taskInfos == null || taskInfos.isEmpty()) {
            return null;
        }
        String taskPath = null;
        for (String taskInfo : taskInfos) {
            try {
                taskPath = ZKPaths.makePath(this.taskQueue, taskType, taskInfo, serverId);
                if (zkclient.checkExists().forPath(taskPath) == null) {
                    continue;
                }
                return taskInfo;
            } catch (Exception e) {
                LOG.error("get task first server happen error path:[{}]", taskPath, e);
            }
        }
        return null;
    }

    private boolean deleteTask(String taskName, String taskType, String taskQueuePath) {
        try {
            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName);
            if (zkclient.checkExists().forPath(path) != null) {
                zkclient.delete().deletingChildrenIfNeeded().forPath(path);
            }
            return true;
        } catch (Exception e) {
            LOG.error("delete task error {}", e);
        }
        return false;
    }

    public int deleteTasks(long deleteTime, String taskType, String taskQueuePath) {
        try {
            Preconditions.checkNotNull(taskQueuePath, "queue is empty !!");
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            List<String> nodes = getTaskList(taskType, taskQueuePath);
            if (nodes == null || nodes.isEmpty()) {
                return 0;
            }

            //循环删除数据
            int count = 0;
            for (String taskName : nodes) {
                TaskModel taskModel = getTaskContentNodeInfo(taskType, taskName, taskQueuePath);
                if (taskModel == null) {
                    continue;
                }
                long ctime = TimeUtils.getMiles(taskModel.getCreateTime());
                if (ctime > deleteTime) {
                    continue;
                }
                if (deleteTask(taskName, taskType, taskQueuePath)) {
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
            LOG.error("delete tasks error {}", e);
        }
        return 0;
    }

    @Override
    public Pair<Integer, Integer> reviseTaskStat(String taskType, long ttl, Collection<String> aliveServers) {
        Pair<Integer, Integer> counts = new Pair<>(0, 0);
        try {
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(aliveServers, "Services is empty !!");
            // 获取子任务名称队列
            List<String> taskQueues = getTaskList(taskType);
            if (taskQueues == null || taskQueues.isEmpty()) {
                return counts;
            }
            // 删除任务
            int deleteCount = deleteTasks(ttl, taskType, taskQueue);
            deleteCount += deleteTasks(ttl, taskType, taskHistory);
            // 维护任务状态
            int reviseCount = reviseTaskState(taskQueues, aliveServers, taskType, deleteCount);
            counts.setFirst(deleteCount);
            counts.setSecond(reviseCount);
            //将维护过后的任务状态为finish的迁移至history
            mvHistoryQueue(taskType, this.taskQueue, this.taskHistory, 86400000);
        } catch (Exception e) {
            LOG.error("revise task error {}", e);
        }
        return counts;
    }

    private int mvHistoryQueue(String taskType, String taskQueue, String historyQueue, long time) throws Exception {
        // 获取子任务名称队列
        List<String> taskQueues = getTaskList(taskType, taskQueue);
        if (taskQueue == null || taskQueue.isEmpty()) {
            return 0;
        }
        int i = 0;
        long currentTime = System.currentTimeMillis();
        for (String taskName : taskQueues) {
            TaskModel taskModel = getTaskContentNodeInfo(taskType, taskName, taskQueue);
            if (TaskState.FINISH.code() != taskModel.getTaskState()) {
                continue;
            }
            long createTime = TimeUtils.getMiles(taskModel.getCreateTime());
            if (currentTime - createTime > time) {
                continue;
            }
            if (taskModel == null) {
                deleteTask(taskName, taskType, taskQueue);
                i++;
                continue;
            }
            String source = ZKPaths.makePath(taskQueue, taskType, taskName);
            String dent = ZKPaths.makePath(historyQueue, taskType, taskName);
            mvData(source, dent, true);
            LOG.info("[{}] task {} is history", taskType, taskName);
            i++;
        }
        return i;
    }

    public void mvData(String sourcePath, String dentPath, boolean overFlag) throws Exception {
        if (zkclient.checkExists().forPath(sourcePath) == null) {
            return;
        }
        List<String> childs = zkclient.getChildren().forPath(sourcePath);
        boolean dentExists = zkclient.checkExists().forPath(dentPath) != null;
        if (dentExists && !overFlag) {
            return;
        }
        if (childs != null) {
            for (String child : childs) {
                mvData(sourcePath + "/" + child, dentPath + "/" + child, overFlag);
            }
        }
        byte[] data = zkclient.getData().forPath(sourcePath);
        if (zkclient.checkExists().forPath(dentPath) != null) {
            zkclient.setData().forPath(dentPath, data);
        } else {
            zkclient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(dentPath, data);
        }
        zkclient.delete().forPath(sourcePath);
    }

    public <T> T getObject(String path, Class<T> clazz) throws Exception {
        if (zkclient.checkExists().forPath(path) == null) {
            return null;
        }
        byte[] data = zkclient.getData().forPath(path);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, clazz);
    }

    public void setPersistentObject(String path, Object obj) throws Exception {
        byte[] data = JsonUtils.toJsonBytes(obj);
        if (zkclient.checkExists().forPath(path) == null) {
            zkclient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);
        } else {
            zkclient.setData().forPath(path, data);
        }
    }

    public List<String> getChilds(String path) throws Exception {
        if (zkclient.checkExists().forPath(path) == null) {
            return new ArrayList<>();
        }
        return zkclient.getChildren().forPath(path);
    }

    /**
     * 概述：维护任务的状态
     *
     * @param taskQueue
     * @param aliveServers
     * @param taskType
     * @param deleteIndex
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private int reviseTaskState(List<String> taskQueue, Collection<String> aliveServers, String taskType, int deleteIndex) {
        int count = 0;
        try {
            int size = taskQueue.size();
            String taskName;
            String tmpPath;
            TaskModel taskContent;
            TaskServerNodeModel taskServer;
            List<String> servers;
            for (int i = (size - 1); i >= deleteIndex; i--) {
                taskName = taskQueue.get(i);
                taskContent = getTaskContentNodeInfo(taskType, taskName, this.taskQueue);
                if (taskContent == null) {
                    LOG.warn("{} {} is null", taskType, taskName);
                    continue;
                }
                // 不为RUN与Exception的任务不进行检查
                int stat = taskContent.getTaskState();
                boolean exceptionFlag = TaskState.EXCEPTION.code() == stat;
                if (!(TaskState.RUN.code() == stat || exceptionFlag)) {
                    continue;
                }
                // 获取任务下的子节点
                tmpPath = ZKPaths.makePath(this.taskQueue, taskType, taskName);
                servers = getChilds(tmpPath);
                // 服务为空，任务标记为异常
                if (servers == null || servers.isEmpty()) {
                    count++;
                    taskContent.setTaskState(TaskState.EXCEPTION.code());
                    updateTaskContentNode(taskContent, taskType, taskName, this.taskQueue);
                    continue;
                }
                boolean isException = false;
                for (String server : servers) {
                    // 存活的server不进行操作
                    if (aliveServers.contains(server)) {
                        continue;
                    }
                    //不存活的server，节点标记为Exception
                    taskServer = getTaskServerContentNodeInfo(taskType, taskName, server, this.taskQueue);
                    if (taskServer == null) {
                        LOG.warn("taskType :{}, taskName :{}, serverId :{} is not exists", taskType, taskName, server);
                        taskServer = new TaskServerNodeModel();
                        taskServer.setTaskState(TaskState.UNKNOW.code());
                    }

                    if (TaskState.FINISH.code() == taskServer.getTaskState()) {
                        continue;
                    }
                    isException = true;
                    taskServer.setTaskState(TaskState.EXCEPTION.code());
                    updateServerTaskContentNode(server, taskName, taskType, taskServer, this.taskQueue);
                }
                if (isException && !exceptionFlag) {
                    count++;
                    taskContent.setTaskState(TaskState.EXCEPTION.code());
                    updateTaskContentNode(taskContent, taskType, taskName, this.taskQueue);
                }

            }
        } catch (Exception e) {
            LOG.error("revise task error {}", e);
        }
        return count;
    }

    @Override
    public TaskModel getTaskContentNodeInfo(String taskType, String taskName) {
        return getTaskContentNodeInfo(taskType, taskName, this.taskQueue);
    }

    private TaskModel getTaskContentNodeInfo(String taskType, String taskName, String taskQueuePath) {
        try {
            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName);
            return getObject(path, TaskModel.class);
        } catch (Exception e) {
            LOG.error("get task content error {}", e);
        }
        return null;
    }

    @Override
    public TaskServerNodeModel getTaskServerContentNodeInfo(String taskType, String taskName, String serverId) {
        return getTaskServerContentNodeInfo(taskType, taskName, serverId, taskQueue);
    }

    public TaskServerNodeModel getTaskServerContentNodeInfo(String taskType, String taskName, String serverId,
                                                            String taskQueuePath) {
        try {

            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName, serverId);
            return getObject(path, TaskServerNodeModel.class);
        } catch (Exception e) {
            LOG.error("get server task content error {}", e);
        }
        return null;
    }

    @Override
    public String getFirstTaskName(String taskType) {
        if (BrStringUtils.isEmpty(taskType)) {
            return null;
        }
        List<String> orderTaskName = getTaskList(taskType);
        if (orderTaskName == null || orderTaskName.isEmpty()) {
            return null;
        }
        return orderTaskName.get(0);
    }

    @Override
    public List<Pair<String, Integer>> getServerStatus(String taskType, String taskName) {
        List<Pair<String, Integer>> serverStatus = new ArrayList<>();
        List<String> childeServers = getTaskServerList(taskType, taskName);
        if (childeServers == null || childeServers.isEmpty()) {
            return serverStatus;
        }
        Pair<String, Integer> stat;
        int istat;
        TaskServerNodeModel tmpServer;
        for (String child : childeServers) {
            stat = new Pair<>();
            tmpServer = getTaskServerContentNodeInfo(taskType, taskName, child);
            stat.setFirst(child);
            istat = tmpServer == null ? -3 : tmpServer.getTaskState();
            stat.setSecond(istat);
            serverStatus.add(stat);
        }
        return serverStatus;
    }

    @Override
    public List<String> getTaskServerList(String taskType, String taskName) {
        return getTaskServerList(taskType, taskName, this.taskQueue);
    }

    public List<String> getTaskServerList(String taskType, String taskName, String taskQueuePath) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            Preconditions.checkNotNull(taskName, "taskName is empty !!");
            Preconditions.checkNotNull(taskQueuePath, "queue path is empty !!");
            String path = ZKPaths.makePath(taskQueuePath, taskType, taskName);
            List<String> childeServers = getChilds(path);
            if (childeServers == null || childeServers.isEmpty()) {
                return ImmutableList.of();
            }
            //升序排列任务
            childeServers.sort(Comparator.naturalOrder());
            return childeServers;
        } catch (Exception e) {
            LOG.error("get task server list happen error", e);
        }
        return ImmutableList.of();
    }

    @Override
    public List<String> getTaskList(String taskType) {
        return getTaskList(taskType, this.taskQueue);
    }

    private List<String> getTaskList(String taskType, String taskQueuePath) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is null");
            String path = ZKPaths.makePath(taskQueuePath, taskType);
            List<String> childNodes = getChilds(path);
            if (childNodes == null || childNodes.isEmpty()) {
                return ImmutableList.of();
            }
            //升序排列任务
            childNodes.sort(Comparator.naturalOrder());
            return childNodes;
        } catch (Exception e) {
            LOG.error("get task {} list happen error ", taskType, e);
        }
        return ImmutableList.of();
    }

    @Override
    public TaskTypeModel getTaskTypeInfo(String taskType) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            String path = ZKPaths.makePath(this.taskQueue, taskType);
            return getObject(path, TaskTypeModel.class);
        } catch (Exception e) {
            LOG.error("get Task [{}] happen error ", taskType, e);
        }
        return null;
    }

    @Override
    public boolean setTaskTypeModel(String taskType, TaskTypeModel type) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is empty !!");
            String path = ZKPaths.makePath(this.taskQueue, taskType);
            setPersistentObject(path, type);
            return true;
        } catch (Exception e) {
            LOG.error("set task [{}] content:[{}] happen error ", taskType, type, e);
        }
        return false;

    }

    @Override
    public List<String> getTransferTask(String taskType) {

        try {
            Preconditions.checkNotNull(taskType, "taskType is null");
            String path = ZKPaths.makePath(this.taskTransfer, taskType);
            return getChilds(path);
        } catch (Exception e) {
            LOG.error("get transfer task [{}] happen error", taskType, e);
        }
        return ImmutableList.of();
    }

    @Override
    public boolean deleteTransferTask(String taskType, String taskName) {
        try {
            Preconditions.checkNotNull(taskType, "TaskType is null");
            Preconditions.checkNotNull(taskName, "TaskName is null");
            String path = ZKPaths.makePath(this.taskTransfer, taskType, taskName);
            if (zkclient.checkExists().forPath(path) == null) {
                return true;
            }
            zkclient.delete().deletingChildrenIfNeeded().forPath(path);
            return true;
        } catch (Exception e) {
            LOG.error("delete transfer task {} {} happen error", taskType, taskName, e);
        }
        return false;
    }

    @Override
    public boolean setTransferTask(String taskType, String taskName) {

        try {
            Preconditions.checkNotNull(taskType, "TaskType is null");
            Preconditions.checkNotNull(taskName, "TaskName is null");
            String path = ZKPaths.makePath(this.taskTransfer, taskType, taskName);
            if (zkclient.checkExists().forPath(path) != null) {
                return false;
            }
            String str = zkclient.create()
                                 .creatingParentsIfNeeded()
                                 .withMode(CreateMode.PERSISTENT)
                                 .forPath(path);
            return !BrStringUtils.isEmpty(str);
        } catch (Exception e) {
            LOG.error("set transfer task {} {} happen error ", taskType, taskName, e);
        }
        return false;
    }

    @Override
    public void recoveryTask(String taskType, String serverId) {
        List<String> needTasks = collectHistoryFailTasks(taskType, serverId, this.taskHistory);
        if (needTasks == null || needTasks.isEmpty()) {
            return;
        }
        for (String taskName : needTasks) {
            String path = ZKPaths.makePath(this.taskHistory, taskType, taskName);
            String dent = ZKPaths.makePath(this.taskQueue, taskType, taskName);
            try {
                TaskModel task = getObject(path, TaskModel.class);
                if (task == null) {
                    this.zkclient.delete().deletingChildrenIfNeeded().forPath(path);
                    LOG.info("[{}] task[{}] is invalid", taskType, taskName);
                    continue;
                }
                task.setTaskState(TaskState.RERUN.code());
                updateTaskContentNode(task, taskType, taskName, this.taskHistory);
                mvData(path, dent, true);
                LOG.info("[{}] task[{}] is recovery", taskType, taskName);
            } catch (Exception e) {
                LOG.error("recovery task {} {} happen error ", taskType, taskName);
            }
        }
    }
}
