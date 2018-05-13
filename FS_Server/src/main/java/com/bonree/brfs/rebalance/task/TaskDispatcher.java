package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.rebalance.BalanceTaskGenerator;
import com.bonree.brfs.rebalance.task.listener.ServerChangeListener;
import com.bonree.brfs.rebalance.task.listener.TaskStatusListener;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午4:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此处来进行任务分配，任务核心控制
 ******************************************************************************/
public class TaskDispatcher {

    private final static Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);

    private final static int DEFAULT_INTERVAL = 30;

    private final static double DEFAULT_PROCESS = 0.6;

    private CuratorClient curatorClient;

    private LeaderLatch leaderLath;

    private ServerIDManager idManager;

    private StorageNameManager snManager;

    private TaskMonitor monitor = new TaskMonitor();;

    private final String baseRebalancePath;

    private final String changesPath;

    private final String tasksPath;

    private final ServiceManager serviceManager;

    private final CuratorTreeCache treeCache;

    private final String virtualRoutePath;

    private final String normalRoutePath;

    private final BalanceTaskGenerator taskGenerator;

    private final AtomicBoolean isLoad = new AtomicBoolean(false);

    private ExecutorService singleServer = Executors.newSingleThreadExecutor();

    private ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);

    // 此处为任务缓存，只有身为leader的server才会进行数据缓存
    private Map<Integer, List<ChangeSummary>> cacheSummaryCache = new ConcurrentHashMap<Integer, List<ChangeSummary>>();

    // 存放当前正在执行的任务
    private Map<Integer, BalanceTaskSummary> runTask = new ConcurrentHashMap<Integer, BalanceTaskSummary>();

    // 为了能够有序的处理变更，需要将变更添加到队列中
    private BlockingQueue<ChangeDetail> detailQueue = new ArrayBlockingQueue<>(256);

    public static class ChangeDetail {

        private final CuratorFramework client;

        private final TreeCacheEvent event;

        public ChangeDetail(CuratorFramework client, TreeCacheEvent event) {
            this.client = client;
            this.event = event;
        }

        public CuratorFramework getClient() {
            return client;
        }

        public TreeCacheEvent getEvent() {
            return event;
        }

    }

    public boolean isEmptyByte(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return true;
        }
        return false;
    }

    public boolean isRemovedNode(TreeCacheEvent event) {
        if (event.getType() == Type.NODE_REMOVED) {
            return true;
        }
        return false;
    }

    public boolean isUpdatedNode(TreeCacheEvent event) {
        if (event.getType() == Type.NODE_UPDATED) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unused")
    private boolean isAddedNode(TreeCacheEvent event) {
        if (event.getType() == Type.NODE_ADDED) {
            return true;
        }
        return false;
    }

    /** 概述：
     * @param client
     * @param event
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void loadCache(CuratorFramework client, TreeCacheEvent event) throws Exception {
        CuratorClient curatorClient = CuratorClient.wrapClient(client);
        String nodePath = event.getData().getPath();
        int lastSepatatorIndex = nodePath.lastIndexOf('/');
        String parentPath = StringUtils.substring(nodePath, 0, lastSepatatorIndex);

        String greatPatentPath = StringUtils.substring(parentPath, 0, parentPath.lastIndexOf('/'));
        List<String> snPaths = curatorClient.getChildren(greatPatentPath); // 此处获得子节点名称
        if (snPaths != null) {
            for (String snNode : snPaths) {
                String snPath = greatPatentPath + Constants.SEPARATOR + snNode;
                List<String> childPaths = curatorClient.getChildren(snPath);

                List<ChangeSummary> changeSummaries = new CopyOnWriteArrayList<>();
                if (childPaths != null) {
                    for (String childNode : childPaths) {
                        String childPath = snPath + Constants.SEPARATOR + childNode;
                        byte[] data = curatorClient.getData(childPath);
                        ChangeSummary cs = JSON.parseObject(data, ChangeSummary.class);
                        changeSummaries.add(cs);
                    }
                }
                // 如果该目录下有服务变更信息，则进行服务变更信息保存
                if (!changeSummaries.isEmpty()) {
                    // 需要对changeSummary进行已时间来排序
                    Collections.sort(changeSummaries);
                    cacheSummaryCache.put(changeSummaries.get(0).getStorageIndex(), changeSummaries);
                }
            }
        }

        // 加载任务缓存
        List<String> sns = curatorClient.getChildren(tasksPath);
        if (sns != null && !sns.isEmpty()) {
            for (String sn : sns) {
                String taskNode = tasksPath + Constants.SEPARATOR + sn + Constants.SEPARATOR + Constants.TASK_NODE;
                if (curatorClient.checkExists(taskNode)) {
                    BalanceTaskSummary bts = JSON.parseObject(curatorClient.getData(taskNode), BalanceTaskSummary.class);
                    runTask.put(Integer.valueOf(sn), bts);
                }
            }
        }
    }

    public void start() throws Exception {

        LOG.info("begin leaderLath server!");
        leaderLath.start();

        LOG.info("changeMonitorPath:" + changesPath);
        treeCache.addListener(changesPath, new ServerChangeListener("server_change", this));

        LOG.info("tasksPath:" + tasksPath);
        treeCache.addListener(tasksPath, new TaskStatusListener("task_status", this));

        singleServer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    dealChangeSDetail();
                } catch (InterruptedException e) {
                    System.out.println("consumer queue error!!");
                    e.printStackTrace();
                }
            }
        });

        scheduleExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (leaderLath.hasLeadership()) {
                    if (isLoad.get()) {
                        for (Entry<Integer, List<ChangeSummary>> entry : cacheSummaryCache.entrySet()) {
                            auditTask(entry.getValue());
                        }
                    }
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

    }

    public TaskDispatcher(final CuratorClient curatorClient, String baseRebalancePath, String baseRoutesPath, ServerIDManager idManager, ServiceManager serviceManager) {
        this.baseRebalancePath = BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRebalancePath, "baseRebalancePath is not null!"));
        this.virtualRoutePath = BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRoutesPath, "baseRoutesPath is not null!")) + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE;
        this.normalRoutePath = BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRoutesPath, "baseRoutesPath is not null!")) + Constants.SEPARATOR + Constants.NORMAL_ROUTE;
        this.changesPath = baseRebalancePath + Constants.SEPARATOR + Constants.CHANGES_NODE;
        this.tasksPath = baseRebalancePath + Constants.SEPARATOR + Constants.TASKS_NODE;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        taskGenerator = new SimpleTaskGenerator();
        this.curatorClient = curatorClient;
        curatorClient.getInnerClient().getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                // 为了保险期间，只要出现网络波动，则需要重新加载缓存
                if (newState == ConnectionState.LOST) {
                    isLoad.set(false);
                } else if (newState == ConnectionState.SUSPENDED) {
                    isLoad.set(false);
                } else if (newState == ConnectionState.RECONNECTED) {
                    isLoad.set(false);
                }
            }
        });

        String leaderPath = this.baseRebalancePath + Constants.SEPARATOR + Constants.DISPATCH_LEADER;
        LOG.info("leader path:" + leaderPath);
        leaderLath = new LeaderLatch(this.curatorClient.getInnerClient(), leaderPath);
        
        leaderLath.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {

            }

            @Override
            public void isLeader() {
                System.out.println("I'am taskDispatch leader!!!!");
            }
        });
        treeCache = CuratorCacheFactory.getTreeCache();

    }

    public void dealChangeSDetail() throws InterruptedException {
        ChangeDetail cd = null;
        while (true) {
            cd = detailQueue.take();
            List<ChangeSummary> changeSummaries = addOneCache(cd.getClient(), cd.getEvent());
            System.out.println("consume:" + changeSummaries);
        }

    }

    public List<ChangeSummary> addOneCache(CuratorFramework client, TreeCacheEvent event) {
        List<ChangeSummary> changeSummaries = null;
        if (event.getData().getData() != null) {
            System.out.println("parse:" + new String(event.getData().getData()));
            ChangeSummary changeSummary = JSON.parseObject(event.getData().getData(), ChangeSummary.class);
            int storageIndex = changeSummary.getStorageIndex();
            changeSummaries = cacheSummaryCache.get(storageIndex);

            if (changeSummaries == null) {
                changeSummaries = new CopyOnWriteArrayList<>();
                cacheSummaryCache.put(storageIndex, changeSummaries);
            }
            if (!changeSummaries.contains(changeSummary)) {
                System.out.println("add cache:" + changeSummary);
                changeSummaries.add(changeSummary);
                System.out.println("cacheSummaryCache:" + cacheSummaryCache);
            }
            LOG.info("changeSummaries:" + changeSummaries);
        }

        return changeSummaries;
    }

    /** 概述：非阻塞审计任务
     * @param changeSummaries
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void auditTask(List<ChangeSummary> changeSummaries) {
        if (changeSummaries == null || changeSummaries.isEmpty()) {
            return;
        }

        System.out.println("audit:" + changeSummaries);

        // 当前有任务在执行
        if (runTask.get(changeSummaries.get(0).getStorageIndex()) != null) {
            checkTask(changeSummaries);
            return;
        }

        // 没有正在执行的任务时，优先处理虚拟迁移任务
        if (changeSummaries != null && !changeSummaries.isEmpty()) {

            trimTask(changeSummaries);

            // 先检查虚拟serverID
            // 没有找到虚拟serverID迁移的任务，执行普通迁移的任务
            if (!dealVirtualTask(changeSummaries)) {
                // String serverId = changeSummary.getChangeServer();
                dealNormalTask(changeSummaries);
            }
        }

    }

    /** 概述：去掉无用的变更
     * @param changeSummaries
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void trimTask(List<ChangeSummary> changeSummaries) {
        System.out.println("trimTask !!!!");
        // 需要清除变更抵消。
        Iterator<ChangeSummary> it1 = changeSummaries.iterator();
        Iterator<ChangeSummary> it2 = changeSummaries.iterator();
        while (it1.hasNext()) {
            ChangeSummary cs1 = it1.next();
            if (cs1.getChangeType() == ChangeType.ADD) {
                while (it2.hasNext()) {
                    ChangeSummary cs2 = it2.next();
                    if (cs2.getChangeType() == ChangeType.REMOVE) {
                        if (cs1.getChangeServer().equals(cs2.getChangeServer())) {
                            changeSummaries.remove(cs2);
                            delChangeSummaryNode(cs2);
                        }
                    }
                }
            }
        }
    }

    /** 概述：处理remove导致的数据迁移
     * @param changeSummaries
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private boolean dealNormalTask(List<ChangeSummary> changeSummaries) {
        /*
         * 根据当时的的情况来判定，决策者如何决定，分为三种
         * 1.该SN正常，未做任何操作
         * 2.该SN正在进行virtual serverID恢复，此时分为两种，1.移除的机器为正在进行virtual ID映射的机器，2.移除的机器为其他参与者的机器
         * 3.该SN正在进行副本丢失迁移，此时会根据副本数来决定迁移是否继续。
         */

        // 检测是否能进行数据恢复。
        ChangeSummary cs = changeSummaries.get(0);
        if (cs.getChangeType().equals(ChangeType.REMOVE)) {
            List<String> aliveFirstIDs = getAliveServices();
            List<String> joinerFirstIDs = cs.getCurrentServers();
            boolean canRecover = isCanRecover(cs, joinerFirstIDs, aliveFirstIDs);
            if (canRecover) {
                List<String> aliveSecondIDs = aliveFirstIDs.stream().map((x) -> idManager.getOtherSecondID(x, cs.getStorageIndex())).collect(Collectors.toList());
                List<String> joinerSecondIDs = joinerFirstIDs.stream().map((x) -> idManager.getOtherSecondID(x, cs.getStorageIndex())).collect(Collectors.toList());
                // 构建任务
                BalanceTaskSummary taskSummary = taskGenerator.genBalanceTask(cs.getChangeID(), cs.getStorageIndex(), cs.getChangeServer(), aliveSecondIDs, joinerSecondIDs, Constants.DEFAULT_DELAY_TIME);
                // 发布任务
                dispatchTask(taskSummary);
                // 加入正在执行的任务的缓存中
                setRunTask(taskSummary.getStorageIndex(), taskSummary);
            } else {
                System.out.println("恢复条件不满足：" + changeSummaries);
            }
        }
        return true;
    }

    private List<String> getAliveServices() {
        return serviceManager.getServiceListByGroup(DiskContext.DEFAULT_DISK_NODE_SERVICE_GROUP).stream().map(Service::getServiceId).collect(Collectors.toList());
    }

    private boolean isCanRecover(ChangeSummary cs, List<String> joinerFirstIDs, List<String> aliveFirstIDs) {
        boolean canRecover = true;
        int replicas = snManager.findStorageName(cs.getStorageIndex()).getReplicateCount();

        // 检查参与者是否都存活
        for (String joiner : joinerFirstIDs) {
            if (!aliveFirstIDs.contains(joiner)) {
                canRecover = false;
                break;
            }
        }
        // 检查目前存活的服务，是否满足副本数
        if (aliveFirstIDs.size() < replicas) {
            canRecover = false;
        }
        return canRecover;
    }

    /** 概述：处理add导致的virtual server id数据迁移
     * @param changeSummaries
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private boolean dealVirtualTask(List<ChangeSummary> changeSummaries) {
        /*
         * 如果changeSummaries大于1，则说明在没完成第一个任务的时候，还发生了其他的变更，
         * 此时需要分情况考虑：
         * 若是目标者server出现问题：则需要放弃本次迁移。
         * 1.是否删除当前任务呢？不能删除，必须找到替代的add server，总之add server优先，其次是remove
         * 2.是否需要删除相应的remove变更呢？应该不能删除，因为该server被利用过一段时间，可能会有数据
         * 3.若下个变更是add变更，则可以继续进行虚拟server迁移
         * 4.若是remove变更，则该sn肯定会继续使用虚拟serverID，remove变更不可能继续执行
         * 若是参与者Server出现问题，则需要停止任务恢复
         * 参与者出现问题：
         * 1.参与者不足的情况，该sn出现异常，之后的变更都不能处理
         * 2.参与者充足的情况，需要重新选择参与者进行执行，remove任务依旧放在最后
         */
        System.out.println("dealVirtualTask !!!!");
        System.out.println(changeSummaries);
        boolean addFlag = false;
        for (ChangeSummary changeSummary : changeSummaries) {
            System.out.println(changeSummary);
            if (changeSummary.getChangeType().equals(ChangeType.ADD)) { // 找到第一个ADD
                addFlag = true;
                String changeID = changeSummary.getChangeID();
                int storageIndex = changeSummary.getStorageIndex();
                List<String> currentFirstIDs = getAliveServices();
                List<String> virtualServerIds = idManager.listNormalVirtualID(changeSummary.getStorageIndex());
                String virtualServersPath = idManager.getVirtualServersPath();
                if (virtualServerIds != null && !virtualServerIds.isEmpty()) {
                    Collections.sort(virtualServerIds);
                    for (String virtualID : virtualServerIds) {
                        // 获取使用该serverID的参与者的firstID。
                        List<String> participators = curatorClient.getChildren(virtualServersPath + Constants.SEPARATOR + storageIndex + Constants.SEPARATOR + virtualID);
                        // 如果当前存活的firstID包括该 virtualID的参与者，那么
                        List<String> selectIds = selectAvailableIDs(currentFirstIDs, participators);

                        if (selectIds != null && !selectIds.isEmpty()) {
                            // 需要寻找一个可以恢复的虚拟serverID，此处选择新来的或者没参与过的

                            String selectID = selectIds.get(0); // TODO选择一个可用的server来进行迁移，如果新来的在可迁移里，则选择新来的，若新来的不在可迁移里，可能为挂掉重启。此时选择？

                            // 构建任务需要使用2级serverid
                            String selectSecondID = idManager.getOtherSecondID(selectID, storageIndex);

                            String secondParticipator = null;
                            List<String> aliveServices = getAliveServices();

                            for (String participator : participators) {
                                if (aliveServices.contains(participator)) {
                                    if (participator.equals(idManager.getFirstServerID())) {
                                        secondParticipator = idManager.getSecondServerID(storageIndex);
                                    } else {
                                        secondParticipator = idManager.getOtherSecondID(participator, storageIndex);
                                    }
                                    break;
                                }
                            }
                            // 构造任务
                            BalanceTaskSummary taskSummary = taskGenerator.genVirtualTask(changeID, storageIndex, virtualID, selectSecondID, secondParticipator, Constants.DEFAULT_DELAY_TIME);
                            // 只在任务节点上创建任务，taskOperator会监听，去执行任务

                            dispatchTask(taskSummary);
                            setRunTask(changeSummary.getStorageIndex(), taskSummary);

                            // 无效化virtualID,直到成功
                            boolean flag = false;
                            do {
                                flag = idManager.invalidVirtualID(taskSummary.getStorageIndex(), virtualID);
                            } while (!flag);
                            // 虚拟serverID置为无效
                            // 虚拟serverID迁移完成，会清理缓存和zk上的任务
                            break;
                        } else {
                            System.out.println("该变更不用参与虚拟迁移:" + changeSummaries);
                            changeSummaries.remove(changeSummary);
                            delChangeSummaryNode(changeSummary);

                        }
                    }
                } else {
                    // 没有使用virtual id ，则不需要进行数据迁移
                    System.out.println("not need to virtual recover!!!");
                    System.out.println(changeSummaries);

                    changeSummaries.remove(changeSummary);
                    delChangeSummaryNode(changeSummary);

                }

            }
            // 处理一个任务即可
            if (addFlag) {
                break;
            }
        }
        return addFlag;
    }

    private void checkTask(List<ChangeSummary> changeSummaries) {
        System.out.println("task check!!!");

        // 获取当前任务信息
        BalanceTaskSummary currentTask = runTask.get(changeSummaries.get(0).getStorageIndex());
        String runChangeID = currentTask.getChangeID();

        // trim change cache 清除变更抵消，只删除remove变更
        Iterator<ChangeSummary> it1 = changeSummaries.iterator();
        Iterator<ChangeSummary> it2 = changeSummaries.iterator();
        while (it1.hasNext()) {
            ChangeSummary cs1 = it1.next();
            if (!cs1.getChangeID().equals(runChangeID)) {
                if (cs1.getChangeType() == ChangeType.ADD) {
                    while (it2.hasNext()) {
                        ChangeSummary cs2 = it2.next();
                        if (!cs2.getChangeID().equals(runChangeID)) {
                            if (cs2.getChangeType() == ChangeType.REMOVE) {
                                if (cs1.getChangeServer().equals(cs2.getChangeServer())) {
                                    changeSummaries.remove(cs2);
                                    delChangeSummaryNode(cs2);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 查找影响当前任务的变更
        if (changeSummaries.size() > 1) {
            String changeID = currentTask.getChangeID();
            // 找到正在执行的变更
            ChangeSummary runChangeSummary = changeSummaries.stream().filter(x -> x.getChangeID().equals(changeID)).findFirst().get();
            System.out.println("run change summary:" + runChangeSummary);
            for (ChangeSummary cs : changeSummaries) {

                if (!cs.getChangeID().equals(runChangeSummary.getChangeID())) { // 与正在执行的变更不同
                    if (runChangeSummary.getChangeType().equals(ChangeType.ADD)) { // 正在执行虚拟迁移任务
                        // 正在执行的任务为虚拟serverID迁移
                        if (cs.getChangeType().equals(ChangeType.REMOVE)) { // 虚拟迁移时，出现问题
                            System.out.println("remove sid:" + cs.getChangeServer());
                            System.out.println("run task sid:" + currentTask.getInputServers().get(0));
                            if (cs.getChangeServer().equals(currentTask.getInputServers().get(0))) {
                                // 用于倒计时
                                int interval = currentTask.getInterval();
                                if (interval == -1) {
                                    currentTask.setInterval(DEFAULT_INTERVAL);
                                } else if (interval == 0) {
                                    List<String> aliveServices = getAliveServices();
                                    String otherFirstID = idManager.getOtherFirstID(currentTask.getInputServers().get(0), currentTask.getStorageIndex());
                                    if (!aliveServices.contains(otherFirstID)) {
                                        if (!currentTask.getTaskStatus().equals(TaskStatus.CANCEL)) {
                                            updateTaskStatus(currentTask, TaskStatus.CANCEL);
                                        } else {
                                            // 下次心跳删除该任务
                                            changeSummaries.remove(runChangeSummary);
                                            delChangeSummaryNode(runChangeSummary);
                                            delBalanceTask(currentTask);
                                            removeRunTask(currentTask.getStorageIndex());
                                            // 将virtual serverID 标为可用
                                            idManager.normalVirtualID(currentTask.getStorageIndex(), currentTask.getServerId());
                                        }
                                    }
                                } else if (interval > 0) {
                                    currentTask.setInterval(currentTask.getInterval() - 1);
                                }

                                break;
                            } else {
                                List<String> joiners = currentTask.getOutputServers();
                                if (joiners.contains(cs.getChangeServer())) { // 参与者挂掉
                                    int interval = currentTask.getInterval();

                                    if (interval == -1) {
                                        currentTask.setInterval(DEFAULT_INTERVAL);
                                    } else if (interval > 0) {
                                        currentTask.setInterval(currentTask.getInterval() - 1);
                                    } else if (interval == 0) {
                                        List<String> aliveServices = getAliveServices();
                                        String otherFirstID = idManager.getOtherFirstID(currentTask.getOutputServers().get(0), runChangeSummary.getStorageIndex());
                                        if (!aliveServices.contains(otherFirstID)) {
                                            // 重新选择
                                            String virtualServersPath = idManager.getVirtualServersPath();
                                            List<String> participators = curatorClient.getChildren(virtualServersPath + Constants.SEPARATOR + currentTask.getStorageIndex() + Constants.SEPARATOR + currentTask.getServerId());
                                            String secondParticipator = null;
                                            for (String participator : participators) {
                                                if (aliveServices.contains(participator)) {
                                                    if (participator.equals(idManager.getFirstServerID())) {
                                                        secondParticipator = idManager.getSecondServerID(currentTask.getStorageIndex());
                                                    } else {
                                                        secondParticipator = idManager.getOtherSecondID(participator, currentTask.getStorageIndex());
                                                    }
                                                    break;
                                                }
                                            }
                                            if (secondParticipator != null) {// 选择成功
                                                delBalanceTask(currentTask);
                                                currentTask.setOutputServers(Lists.newArrayList(secondParticipator));
                                                dispatchTask(currentTask);
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    } else if (runChangeSummary.getChangeType().equals(ChangeType.REMOVE)) { // 正在执行普通迁移任务
                        if (cs.getChangeType().equals(ChangeType.ADD)) {
                            // 正在执行的任务为remove恢复，检测到ADD时间，并且是同一个serverID
                            if (cs.getChangeServer().equals(runChangeSummary.getChangeServer())) {
                                String taskPath = tasksPath + Constants.SEPARATOR + runChangeSummary.getStorageIndex() + Constants.SEPARATOR + Constants.TASK_NODE;
                                // 任务进度小于指定进度，则终止任务
                                if (currentTask.getTaskStatus().equals(TaskStatus.CANCEL)) {
                                    // 下次心跳删除该任务
                                    changeSummaries.remove(runChangeSummary);
                                    delChangeSummaryNode(runChangeSummary);
                                    delBalanceTask(currentTask);
                                    removeRunTask(currentTask.getStorageIndex());
                                    break;
                                }
                                if (monitor.getTaskProgress(curatorClient, taskPath) < DEFAULT_PROCESS) {
                                    if (!currentTask.getTaskStatus().equals(TaskStatus.CANCEL)) {
                                        updateTaskStatus(currentTask, TaskStatus.CANCEL);
                                    }
                                    break;
                                }
                            } else {
                                // 如果任务暂停，查看回来的是否为增经的参与者
                                if (currentTask.getTaskStatus().equals(TaskStatus.PAUSE)) {
                                    List<String> aliverServers = getAliveServices();
                                    // 参与者和接收者都存活
                                    if (aliverServers.containsAll(currentTask.getOutputServers()) && aliverServers.containsAll(currentTask.getInputServers())) {
                                        updateTaskStatus(currentTask, TaskStatus.RUNNING);
                                    }
                                }
                            }
                        } else if (cs.getChangeType().equals(ChangeType.REMOVE)) {
                            // 有可能是参与者挂掉，参与者包括接收者和发送者
                            // 参与者停止恢复 停止恢复必须查看是否有
                            String secondID = cs.getChangeServer();
                            List<String> joiners = currentTask.getOutputServers();
                            List<String> receivers = currentTask.getInputServers();
                            if (joiners.contains(secondID)) { // 参与者出现问题 或 既是参与者又是接收者
                                if (!TaskStatus.PAUSE.equals(currentTask.getTaskStatus())) {
                                    updateTaskStatus(currentTask, TaskStatus.PAUSE);
                                }
                                break;
                            } else if (receivers.contains(secondID)) {// 纯接收者，需要重选
                                if (!TaskStatus.PAUSE.equals(currentTask.getTaskStatus())) {
                                    updateTaskStatus(currentTask, TaskStatus.PAUSE);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    public void delChangeSummaryNode(ChangeSummary summary) {
        System.out.println("delete:" + summary);
        String path = changesPath + Constants.SEPARATOR + summary.getStorageIndex() + Constants.SEPARATOR + summary.getChangeID();
        curatorClient.guaranteedDelete(path, false);
    }

    public void delBalanceTask(BalanceTaskSummary task) {
        System.out.println("delete task:" + task);
        String taskNode = tasksPath + Constants.SEPARATOR + task.getStorageIndex() + Constants.SEPARATOR + Constants.TASK_NODE;
        curatorClient.delete(taskNode, true);
    }

    public void updateTaskStatus(BalanceTaskSummary task, TaskStatus status) {
        task.setTaskStatus(status);
        String taskNode = tasksPath + Constants.SEPARATOR + task.getStorageIndex() + Constants.SEPARATOR + Constants.TASK_NODE;
        curatorClient.setData(taskNode, JSON.toJSONBytes(task));
    }

    /** 概述：在任务节点上创建任务
     * @param taskSummary
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean dispatchTask(BalanceTaskSummary taskSummary) {
        int storageIndex = taskSummary.getStorageIndex();
        String jsonStr = JSON.toJSONString(taskSummary);
        System.out.println("create task:" + jsonStr);
        // 创建任务
        String taskNode = tasksPath + Constants.SEPARATOR + storageIndex + Constants.SEPARATOR + Constants.TASK_NODE;
        if (!curatorClient.checkExists(taskNode)) {
            curatorClient.createPersistent(taskNode, true, jsonStr.getBytes());
        }
        return true;
    }

    private static List<String> selectAvailableIDs(List<String> currentFirstIDs, List<String> participators) {
        List<String> availableIDs = null;
        if (currentFirstIDs.containsAll(participators)) {
            availableIDs = new ArrayList<>();
            for (String firstID : currentFirstIDs) {
                if (!participators.contains(firstID)) {
                    availableIDs.add(firstID);
                }
            }
        }
        return availableIDs;
    }

    public LeaderLatch getLeaderLatch() {
        return leaderLath;
    }

    public AtomicBoolean isLoad() {
        return isLoad;
    }

    public BlockingQueue<ChangeDetail> getDetailQueue() {
        return detailQueue;
    }

    public String getVirualRoutePath() {
        return virtualRoutePath;
    }

    public String getNormalRoutePath() {
        return normalRoutePath;
    }

    public String getChangesPath() {
        return changesPath;
    }

    public Map<Integer, List<ChangeSummary>> getSummaryCache() {
        return cacheSummaryCache;
    }

    public ServerIDManager getServerIDManager() {
        return idManager;
    }

    public void setRunTask(int storageIndex, BalanceTaskSummary task) {
        runTask.put(storageIndex, task);
    }

    public void removeRunTask(int storageIndex) {
        runTask.remove(storageIndex);
    }

    public static void main(String[] args) throws Exception {
        List<String> l1 = Lists.newArrayList("1", "2", "3", "4", "5");
        List<String> l2 = Lists.newArrayList("1", "2", "3");
        System.out.println(selectAvailableIDs(l1, l2));
    }

}
