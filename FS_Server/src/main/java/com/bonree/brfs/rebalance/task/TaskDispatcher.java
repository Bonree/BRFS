package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.BalanceTaskGenerator;
import com.bonree.brfs.rebalance.Constants;
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

    private CuratorClient curatorClient;

    private LeaderLatch leaderLath;

    private ServerIDManager idManager;

    // private TaskMonitor monitor;

    private final String baseRebalancePath;

    private final String changesPath;

    private final String tasksPath;

    private final ServiceManager serviceManager;

    private final CuratorTreeCache treeCache;

    private final String virtualRoutePath;
    private final String normalRoutePath;

    private final BalanceTaskGenerator taskGenerator;

    private final AtomicBoolean isLoad = new AtomicBoolean(true);

    private ExecutorService singleServer = Executors.newSingleThreadExecutor();

    // 此处为任务缓存，只有身为leader的server才会进行数据缓存
    private Map<Integer, List<ChangeSummary>> cacheSummaryCache = new ConcurrentHashMap<Integer, List<ChangeSummary>>();

    private Map<Integer, Boolean> dealFlag = new ConcurrentHashMap<Integer, Boolean>();

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
        String nodePath = event.getData().getPath();
        int lastSepatatorIndex = nodePath.lastIndexOf('/');
        String parentPath = StringUtils.substring(nodePath, 0, lastSepatatorIndex);

        String greatPatentPath = StringUtils.substring(parentPath, 0, parentPath.lastIndexOf('/'));
        List<String> snPaths = client.getChildren().forPath(greatPatentPath); // 此处获得子节点名称
        if (snPaths != null) {
            for (String snNode : snPaths) {
                String snPath = greatPatentPath + Constants.SEPARATOR + snNode;
                List<String> childPaths = client.getChildren().forPath(snPath);

                List<ChangeSummary> changeSummaries = new CopyOnWriteArrayList<>();
                if (childPaths != null) {
                    for (String childNode : childPaths) {
                        String childPath = snPath + Constants.SEPARATOR + childNode;
                        byte[] data = client.getData().forPath(childPath);
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
    }

    public void start() throws Exception {

        LOG.info("begin leaderLath server!");
        leaderLath.start();

        LOG.info("changeMonitorPath:" + changesPath);
        treeCache.addListener(changesPath, new ServerChangeListener("server_change", this));

        LOG.info("tasksPath:" + tasksPath);
        treeCache.addListener(tasksPath, new TaskStatusListener("task_status", this));

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
                    isLoad.set(true);
                } else if (newState == ConnectionState.SUSPENDED) {
                    isLoad.set(true);
                } else if (newState == ConnectionState.RECONNECTED) {
                    isLoad.set(true);
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
    }

    public void dealChangeSDetail() throws InterruptedException {
        ChangeDetail cd = null;
        while (true) {
            cd = detailQueue.take();
            List<ChangeSummary> changeSummaries = addOneCache(cd.getClient(), cd.getEvent());
            System.out.println("consume:" + changeSummaries);
            auditTask(changeSummaries);
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

    public void auditTask(List<ChangeSummary> changeSummaries) {
        if (changeSummaries == null || changeSummaries.isEmpty()) {
            return;
        }
        boolean addFlag = false;
        System.out.println("audit:" + changeSummaries);

        // 为true 不做判断
        if (dealFlag.get(changeSummaries.get(0).getStorageIndex()) != null && dealFlag.get(changeSummaries.get(0).getStorageIndex())) {
            return;
        }

        if (changeSummaries != null && !changeSummaries.isEmpty()) {
            // 先检查虚拟serverID
            for (ChangeSummary changeSummary : changeSummaries) {
                if (changeSummary.getChangeType().equals(ChangeType.ADD)) { // 找到第一个ADD
                    addFlag = true;

                    setStorageFlag(changeSummary.getStorageIndex(), true);
                    String changeID = changeSummary.getChangeID();
                    int storageIndex = changeSummary.getStorageIndex();

                    // String serverID = changeSummary.getChangeServer();

                    List<String> currentFirstIDs = changeSummary.getCurrentServers();
                    if (changeSummaries.size() == 1) { // 首次触发虚拟server ID迁移
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
                                    List<String> aliveServices = serviceManager.getServiceListByGroup("discover").stream().map(Service::getServiceId).collect(Collectors.toList());

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
                                    BalanceTaskSummary taskSummary = taskGenerator.genVirtualTask(changeID, storageIndex, virtualID, selectSecondID, secondParticipator);
                                    // 只在任务节点上创建任务，taskOperator会监听，去执行任务
                                    dispatchTask(taskSummary);
                                    boolean flag = false;
                                    do {
                                        flag = idManager.invalidVirtualID(taskSummary.getStorageIndex(), virtualID);
                                    } while (!flag);
                                    // 虚拟serverID置为无效
                                    // 虚拟serverID迁移完成，会清理缓存和zk上的任务

                                    break;
                                }
                            }
                        } else {
                            // 没有使用virtual id ，则不需要进行数据迁移
                            System.out.println("not need to virtual recover!!!");
                            System.out.println(changeSummaries);

                            changeSummaries.remove(changeSummary);
                            delChangeSummaryNode(changeSummary);

                            // 重新审计
                            auditTask(changeSummaries);
                        }
                    }
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
                    else if (changeSummaries.size() > 1) {
                        for (int i = 1; i < changeSummaries.size(); i++) {
                            // 正在迁移的任务，可能会遇到目标server或者参与者server移除的情况
                            if (changeSummaries.get(i).getChangeType() == ChangeType.REMOVE) {
                                // 正在执行的任务，看是否属于其中的任务
                            }
                        }
                    }

                }
                // 处理一个任务即可
                if (addFlag) {
                    break;
                }
            }
            if (!addFlag) {
                // String serverId = changeSummary.getChangeServer();
                /*
                 * 根据当时的的情况来判定，决策者如何决定，分为三种
                 * 1.该SN正常，未做任何操作
                 * 2.该SN正在进行virtual serverID恢复，此时分为两种，1.移除的机器为正在进行virtual ID映射的机器，2.移除的机器为其他参与者的机器
                 * 3.该SN正在进行副本丢失迁移，此时会根据副本数来决定迁移是否继续。
                 */
                // for (int i = 1; i < changeSummaries.size(); i++) {
                // ChangeSummary tmp = changeSummaries.get(i);
                // String tempServerId = tmp.getChangeServer();
                // if (StringUtils.equals(serverId, tempServerId)) {
                // if (tmp.getChangeType() == ChangeType.ADD) {
                // BalanceTaskSummary taskSummary = taskGenerator.genBalanceTask(changeSummary);
                // if (monitor.getTaskProgress(taskSummary) < 0.6) { // TODO 0.6暂时填充
                // cancelTask(taskSummary); // TODO 此处需要同步,为了一致性，不能是简单的修改任务状态
                // auditTask(changeSummaries);
                // }
                // }
                // }
                //
                // }
                // BalanceTaskSummary taskSummary = taskGenerator.genBalanceTask(changeSummary);
                // dispatchTask(taskSummary);
                System.out.println(changeSummaries);
                System.out.println("no data!!!");
                ChangeSummary deleteSummary = changeSummaries.remove(0);
                delChangeSummaryNode(deleteSummary);
                auditTask(changeSummaries);
            }
        }

    }

    void delChangeSummaryNode(ChangeSummary summary) {
        System.out.println("delete:" + summary);
        String path = changesPath + Constants.SEPARATOR + summary.getStorageIndex() + Constants.SEPARATOR + summary.getChangeID();
        curatorClient.guaranteedDelete(path, false);
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

    public boolean cancelTask(BalanceTaskSummary taskSummary) {
        int storageIndex = taskSummary.getStorageIndex();
        String serverId = taskSummary.getServerId();
        // 设置任务状态
        taskSummary.setTaskStatus(TaskStatus.CANCEL);
        String taskNode = tasksPath + Constants.SEPARATOR + storageIndex + Constants.SEPARATOR + serverId;
        String jsonStr = JSON.toJSONString(taskSummary);
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

    public void setStorageFlag(int storageIndex, boolean flag) {
        dealFlag.put(storageIndex, flag);
    }

    public static void main(String[] args) throws Exception {
        List<String> l1 = Lists.newArrayList("1", "2", "3", "4", "5");
        List<String> l2 = Lists.newArrayList("1", "2", "3");
        System.out.println(selectAvailableIDs(l1, l2));
    }

}
