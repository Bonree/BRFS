package com.bonree.brfs.schedulers.task.manager.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorConfig;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReleaseTask implements MetaTaskManagerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultReleaseTask.class);
    private static final String QUEUE = "queue";
    private static final String TRANSFER = "transfer";
    private static final String HISTORY = "history";
    private String zkUrl = null;
    private String taskRootPath = null;
    private ZookeeperClient client = null;
    private String taskQueue = null;
    private String taskTransfer = null;
    private String taskHistory = null;

    @Inject
    public DefaultReleaseTask(CuratorConfig curatorConfig, ZookeeperPaths zkPath) {
        this(curatorConfig.getAddresses(), zkPath.getBaseTaskPath(), zkPath.getBaseLocksPath());
    }

    public DefaultReleaseTask(String zkUrl, String taskRootPath, String lockPath) {
        this.zkUrl = zkUrl;
        this.taskRootPath = taskRootPath;
        if (BrStringUtils.isEmpty(this.zkUrl)) {
            throw new NullPointerException("zookeeper address is empty");
        }
        if (BrStringUtils.isEmpty(this.taskRootPath)) {
            throw new NullPointerException("task root path is empty");
        }
        if (BrStringUtils.isEmpty(lockPath)) {
            throw new NullPointerException("task lock path is empty");
        }
        this.taskQueue = this.taskRootPath + "/" + QUEUE;
        this.taskTransfer = this.taskRootPath + "/" + TRANSFER;
        this.taskHistory = this.taskRootPath + "/" + HISTORY;
        client = CuratorClient.getClientInstance(this.zkUrl);
    }

    @Override
    public String updateTaskContentNode(TaskModel data, String taskType, String taskName) {
        return updateTaskContentNode(data, taskType, taskName, this.taskQueue);
    }

    private String updateTaskContentNode(TaskModel data, String taskType, String taskName, String taskQueuePath) {
        String pathNode = null;
        try {
            if (data == null) {
                LOG.warn("task content is empty");
                return null;
            }
            byte[] datas = JsonUtils.toJsonBytes(data);
            if (datas == null || datas.length == 0) {
                LOG.warn("task content convert is empty");
                return null;
            }
            if (BrStringUtils.isEmpty(taskType)) {
                LOG.warn("task type is empty");
                return null;
            }
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
            if (!BrStringUtils.isEmpty(taskName) && client.checkExists(taskPath)) {
                client.setData(taskPath, datas);
                return taskName;
            }
            pathNode = client.createPersistentSequential(taskPath, true, datas);
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
            if (BrStringUtils.isEmpty(taskName)) {
                return -1;
            }
            if (BrStringUtils.isEmpty(taskType)) {
                return -2;
            }
            StringBuilder taskPath = new StringBuilder();
            taskPath.append(taskQueuePath).append("/").append(taskType).append("/").append(taskName);
            String path = taskPath.toString();
            if (!client.checkExists(path)) {
                return -3;
            }
            byte[] data = client.getData(path);
            if (data == null || data.length == 0) {
                return -4;
            }
            TaskModel tmp = JsonUtils.toObject(data, TaskModel.class);
            return tmp.getTaskState();
        } catch (Exception e) {
            LOG.error("query task status error {}", e);
        }
        return -5;
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
            if (BrStringUtils.isEmpty(serverId)) {
                return false;
            }
            if (BrStringUtils.isEmpty(taskName)) {
                return false;
            }
            if (BrStringUtils.isEmpty(taskType)) {
                return false;
            }
            if (data == null) {
                LOG.warn("task content is empty");
                return false;
            }
            byte[] datas = JsonUtils.toJsonBytes(data);
            if (datas == null || datas.length == 0) {
                LOG.warn("task content convert is empty");
                return false;
            }
            StringBuilder taskPath = new StringBuilder();
            taskPath.append(taskQueuePath).append("/").append(taskType).append("/").append(taskName).append("/").append(serverId);
            String path = taskPath.toString();
            if (client.checkExists(path)) {
                client.setData(path, datas);
            } else {
                client.createPersistent(path, true, datas);
            }
            return true;
        } catch (Exception e) {
            LOG.error("update server task status error {}", e);
        }
        return false;
    }

    @Override
    public boolean changeTaskContentNodeState(String taskName, String taskType, int taskState) {
        try {
            if (BrStringUtils.isEmpty(taskName)) {
                return false;
            }
            if (BrStringUtils.isEmpty(taskType)) {
                return false;
            }
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
            List<String> taskInfos = getTaskList(taskType);
            if (taskInfos == null || taskInfos.isEmpty()) {
                return null;
            }
            int maxIndex = taskInfos.size() - 1;
            StringBuilder path;
            String taskName;
            String taskPath;
            TaskServerNodeModel tmpR;
            for (int i = maxIndex; i >= 0; i--) {
                taskName = taskInfos.get(i);
                path = new StringBuilder();
                path.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName).append("/").append(
                    serverId);
                taskPath = path.toString();
                if (!client.checkExists(taskPath)) {
                    continue;
                }
                tmpR = getTaskServerContentNodeInfo(taskType, taskName, serverId);
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
        TaskServerNodeModel model = getTaskServer(queue, taskType, checkName, serverId);
        if (model == null || TaskState.EXCEPTION.code() == model.getTaskState()) {
            return true;
        } else {
            return false;
        }
    }

    private TaskServerNodeModel getTaskServer(String queue, String taskType, String name, String server) {
        String serverPath = createServerTaskPath(queue, taskType, name, server);
        if (!client.checkExists(serverPath)) {
            return null;
        }
        byte[] data = client.getData(serverPath);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, TaskServerNodeModel.class);
    }

    private String createTaskPath(String queue, String taskType, String name) {
        return new StringBuilder(queue).append("/").append(taskType).append("/").append(name).toString();
    }

    private String createServerTaskPath(String queue, String taskType, String name, String server) {
        return new StringBuilder(createTaskPath(queue, taskType, name)).append("/").append(server).toString();
    }

    @Override
    public String getFirstServerTask(String taskType, String serverId) {
        List<String> taskInfos = getTaskList(taskType);
        if (taskInfos == null || taskInfos.isEmpty()) {
            return null;
        }
        StringBuilder path;
        String taskPath;
        for (String taskInfo : taskInfos) {
            path = new StringBuilder();
            path.append(this.taskQueue).append("/").append(taskType).append("/").append(taskInfo).append("/").append(serverId);
            taskPath = path.toString();
            if (!client.checkExists(taskPath)) {
                continue;
            }
            return taskInfo;
        }
        return null;
    }

    private boolean deleteTask(String taskName, String taskType, String taskQueuePath) {
        try {
            String path = createTaskPath(taskQueuePath, taskType, taskName);
            if (client.checkExists(path)) {
                client.delete(path, true);
            }
            return true;
        } catch (Exception e) {
            LOG.error("delete task error {}", e);
        }
        return false;
    }

    public int deleteTasks(long deleteTime, String taskType, String taskQueuePath) {
        try {
            if (BrStringUtils.isEmpty(taskType)) {
                return -1;
            }
            List<String> nodes = getTaskList(taskType, taskQueuePath);
            if (nodes == null || nodes.isEmpty()) {
                return 0;
            }
            int size = nodes.size();
            if (size == 0) {
                return 0;
            }
            long firstTime = getTaskCreateTime(nodes.get(0), taskType, taskQueuePath);
            if (deleteTime < firstTime) {
                return 0;
            }
            //循环删除数据
            int count = 0;
            long ctime;
            for (String taskName : nodes) {
                if (BrStringUtils.isEmpty(taskName)) {
                    continue;
                }
                ctime = getTaskCreateTime(taskName, taskType, taskQueuePath);
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
        return -1;
    }

    private long getTaskCreateTime(String taskName, String taskType, String taskQueuePath) {
        try {
            if (BrStringUtils.isEmpty(taskName)) {
                return -1;
            }
            if (BrStringUtils.isEmpty(taskType)) {
                return -2;
            }
            String path = taskQueuePath + "/" + taskType + "/" + taskName;
            if (!client.checkExists(path)) {
                return -3;
            }
            byte[] data;
            data = client.getData(path);
            if (data == null || data.length == 0) {
                return -4;
            }
            TaskModel taskInfo = JsonUtils.toObject(data, TaskModel.class);
            String createTime = taskInfo.getCreateTime();
            if (BrStringUtils.isEmpty(createTime)) {
                return 0;
            } else {
                return TimeUtils.getMiles(createTime, TimeUtils.TIME_MILES_FORMATE);
            }
        } catch (Exception e) {
            LOG.error("get create time error {}", e);
        }
        return -5;
    }

    @Override
    public boolean isInit() {
        if (this.client == null) {
            return false;
        }
        if (BrStringUtils.isEmpty(this.zkUrl)) {
            return false;
        }
        return true;
    }

    @Override
    public Pair<Integer, Integer> reviseTaskStat(String taskType, long ttl, Collection<String> aliveServers) {
        Pair<Integer, Integer> counts = new Pair<>(0, 0);
        try {
            if (BrStringUtils.isEmpty(taskType)) {
                throw new NullPointerException("taskType is empty");
            }
            if (aliveServers == null || aliveServers.isEmpty()) {
                throw new NullPointerException("alive servers is empty");
            }
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
            mvHistoryQueue(taskType, this.taskQueue, this.taskHistory);
        } catch (Exception e) {
            LOG.error("revise task error {}", e);
        }
        return counts;
    }

    private int mvHistoryQueue(String taskType, String taskQueue, String historyQueue) {
        // 获取子任务名称队列
        List<String> taskQueues = getTaskList(taskType, taskQueue);
        if (taskQueue == null || taskQueue.isEmpty()) {
            return 0;
        }
        int i = 0;
        for (String taskName : taskQueues) {
            TaskModel taskModel = getTaskContentNodeInfo(taskType, taskName, taskQueue);
            if (TaskState.FINISH.code() != taskModel.getTaskState()) {
                continue;
            }
            if (taskModel == null) {
                deleteTask(taskName, taskType, taskQueue);
                i++;
                continue;
            }
            mvHistory(taskType, taskName);
            LOG.info("[{}] task {} is history", taskType, taskName);
            i++;
        }
        return i;
    }

    private void mvHistory(String taskType, String taskName) {
        String source = createTaskPath(this.taskQueue, taskType, taskName);
        String dent = createTaskPath(this.taskHistory, taskType, taskName);
        mvData(source, dent, true);
    }

    public void mvData(String sourcePath, String dentPath, boolean overFlag) {
        if (!client.checkExists(sourcePath)) {
            return;
        }
        List<String> childs = client.getChildren(sourcePath);
        boolean dentExists = client.checkExists(dentPath);
        if (dentExists && !overFlag) {
            return;
        }
        if (childs != null) {
            childs.stream().forEach(x -> {
                mvData(sourcePath + "/" + x, dentPath + "/" + x, overFlag);
            });
        }
        byte[] data = client.getData(sourcePath);
        if (client.checkExists(dentPath)) {
            client.setData(dentPath, data);
        } else {
            client.createPersistent(dentPath, true, data);
        }
        client.delete(sourcePath, false);
    }

    public <T> T getObject(String path, Class<T> clazz) {
        if (!client.checkExists(path)) {
            return null;
        }
        byte[] data = client.getData(path);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, clazz);
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
            StringBuilder taskPath;
            String taskName;
            String tmpPath;
            TaskModel taskContent;
            TaskServerNodeModel taskServer;
            List<String> servers;
            for (int i = (size - 1); i >= deleteIndex; i--) {
                taskName = taskQueue.get(i);
                taskContent = getTaskContentNodeInfo(taskType, taskName);
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
                taskPath = new StringBuilder();
                taskPath.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName);
                tmpPath = taskPath.toString();
                servers = client.getChildren(tmpPath);
                // 服务为空，任务标记为异常
                if (servers == null || servers.isEmpty()) {
                    count++;
                    taskContent.setTaskState(TaskState.EXCEPTION.code());
                    updateTaskContentNode(taskContent, taskType, taskName);
                    continue;
                }
                boolean isException = false;
                for (String server : servers) {
                    // 存活的server不进行操作
                    if (aliveServers.contains(server)) {
                        continue;
                    }
                    //不存活的server，节点标记为Exception
                    taskServer = getTaskServerContentNodeInfo(taskType, taskName, server);
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
                    updateServerTaskContentNode(server, taskName, taskType, taskServer);
                }
                if (isException && !exceptionFlag) {
                    count++;
                    taskContent.setTaskState(TaskState.EXCEPTION.code());
                    updateTaskContentNode(taskContent, taskType, taskName);
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
            String path = createTaskPath(taskQueuePath, taskType, taskName);
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
            String path = createServerTaskPath(taskQueuePath, taskType, taskName, serverId);
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
        List<String> childeServers = new ArrayList<>();
        if (BrStringUtils.isEmpty(taskType)) {
            return childeServers;
        }
        if (BrStringUtils.isEmpty(taskName)) {
            return childeServers;
        }
        String path = taskQueuePath + "/" + taskType + "/" + taskName;
        childeServers = client.getChildren(path);
        if (childeServers == null || childeServers.isEmpty()) {
            return childeServers;
        }
        //升序排列任务
        childeServers.sort(Comparator.naturalOrder());
        return childeServers;
    }

    @Override
    public List<String> getTaskList(String taskType) {
        return getTaskList(taskType, this.taskQueue);
    }

    public List<String> getTaskList(String taskType, String taskQueuePath) {
        if (StringUtils.isEmpty(taskType)) {
            throw new NullPointerException("taskType is empty");
        }
        String path = taskQueuePath + "/" + taskType;
        if (!client.checkExists(path)) {
            return null;
        }
        List<String> childNodes = client.getChildren(path);
        if (childNodes == null || childNodes.isEmpty()) {
            return childNodes;
        }
        //升序排列任务
        childNodes.sort(Comparator.naturalOrder());
        return childNodes;
    }

    @Override
    public TaskTypeModel getTaskTypeInfo(String taskType) {
        if (BrStringUtils.isEmpty(taskType)) {
            return null;
        }
        String path = this.taskQueue + "/" + taskType;
        if (!client.checkExists(path)) {
            System.out.println(path);
            return null;
        }
        byte[] data = client.getData(path);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.toObjectQuietly(data, TaskTypeModel.class);
    }

    @Override
    public boolean setTaskTypeModel(String taskType, TaskTypeModel type) {
        if (BrStringUtils.isEmpty(taskType)) {
            return false;
        }
        if (type == null) {
            return false;
        }
        byte[] data = JsonUtils.toJsonBytesQuietly(type);
        if (data == null || data.length == 0) {
            return false;
        }
        String path = this.taskQueue + "/" + taskType;
        if (client.checkExists(path)) {
            client.setData(path, data);
        } else {
            client.createPersistent(path, true, data);
        }
        return true;
    }

    @Override
    public List<String> getTransferTask(String taskType) {
        if (BrStringUtils.isEmpty(taskType)) {
            return null;
        }
        String path = this.taskTransfer + "/" + taskType;
        if (!client.checkExists(path)) {
            return null;
        }
        return client.getChildren(path);
    }

    @Override
    public boolean deleteTransferTask(String taskType, String taskName) {
        if (BrStringUtils.isEmpty(taskType)) {
            return false;
        }
        if (BrStringUtils.isEmpty(taskName)) {
            return false;
        }
        String path = this.taskTransfer + "/" + taskType + "/" + taskName;
        if (!client.checkExists(path)) {
            return true;
        }
        client.delete(path, true);
        return true;
    }

    @Override
    public boolean setTransferTask(String taskType, String taskName) {
        if (BrStringUtils.isEmpty(taskType)) {
            return false;
        }
        if (BrStringUtils.isEmpty(taskName)) {
            return false;
        }
        String path = this.taskTransfer + "/" + taskType + "/" + taskName;
        if (client.checkExists(path)) {
            return false;
        }
        String str = client.createPersistent(path, true);
        return !BrStringUtils.isEmpty(str);
    }

    @Override
    public void recoveryTask(String taskType, String serverId) {
        List<String> needTasks = collectHistoryFailTasks(taskType, serverId, this.taskHistory);
        if (needTasks == null || needTasks.isEmpty()) {
            return;
        }
        needTasks.stream().parallel().forEach(
            taskName -> {
                String path = createTaskPath(this.taskHistory, taskType, taskName);
                String dent = createTaskPath(this.taskQueue, taskType, taskName);
                TaskModel task = getObject(path, TaskModel.class);
                if (task == null) {
                    this.client.delete(path, true);
                    LOG.info("[{}] task[{}] is invalid", taskType, taskName);
                    return;
                }
                task.setTaskState(TaskState.RERUN.code());
                updateTaskContentNode(task, taskType, taskName, this.taskHistory);
                mvData(path, dent, true);
                LOG.info("[{}] task[{}] is recovery", taskType, taskName);
            }
        );
    }
}
