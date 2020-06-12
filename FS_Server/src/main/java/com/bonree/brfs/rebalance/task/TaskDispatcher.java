package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.RebalanceUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalance.BalanceTaskGenerator;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.task.listener.ServerChangeListener;
import com.bonree.brfs.rebalance.task.listener.TaskStatusListener;
import com.bonree.mail.worker.MailWorker;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月23日 下午4:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此处来进行任务分配，任务核心控制
 ******************************************************************************/
public class TaskDispatcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);

    private static final int DEFAULT_INTERVAL = 30;
    private static final double DEFAULT_PROCESS = 0.6;

    private CuratorFramework client;
    private LeaderLatch leaderLath;
    private IDSManager idManager;
    private StorageRegionManager snManager;
    private TaskMonitor monitor = new TaskMonitor();

    private final String changesPath;
    private final String changesHistoryPath;
    private final String tasksPath;
    private final String tasksHistoryPath;
    private final ServiceManager serviceManager;
    private final CuratorTreeCache treeCache;
    private final String virtualRoutePath;
    private final String normalRoutePath;
    private final BalanceTaskGenerator taskGenerator;
    private DiskPartitionInfoManager partitionInfoManager;
    private ClusterConfig clusterConfig;

    private int virtualDelay;
    private int normalDelay;

    private Lock lock = new ReentrantLock();
    private final AtomicBoolean isLoad = new AtomicBoolean(false);

    private ExecutorService singleServer = Executors.newSingleThreadExecutor(new PooledThreadFactory("queue_to_cache"));

    private ScheduledExecutorService scheduleExecutor =
        Executors.newScheduledThreadPool(1, new PooledThreadFactory("audit_task"));

    // 此处为任务缓存，只有身为leader的server才会进行数据缓存
    private Map<Integer, List<DiskPartitionChangeSummary>> changeSummaryCache = new ConcurrentHashMap<>();

    // 存放当前正在执行的任务
    private Map<Integer, BalanceTaskSummary> runTask = new ConcurrentHashMap<>();

    // 为了能够有序的处理变更，需要将变更添加到队列中
    private BlockingQueue<DiskPartitionChangeSummary> detailQueue = new ArrayBlockingQueue<>(256);

    public TaskDispatcher(CuratorFramework client, String baseRebalancePath, String baseRoutesPath,
                          IDSManager idManager, ServiceManager serviceManager, StorageRegionManager snManager, int virtualDelay,
                          int normalDelay, DiskPartitionInfoManager partitionInfoManager, ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        String balancePath =
            BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRebalancePath, "baseRebalancePath is not null!"));
        this.virtualRoutePath =
            BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRoutesPath, "baseRoutesPath is not null!"))
                + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE;
        this.normalRoutePath =
            BrStringUtils.trimBasePath(Preconditions.checkNotNull(baseRoutesPath, "baseRoutesPath is not null!"))
                + Constants.SEPARATOR + Constants.NORMAL_ROUTE;
        this.changesPath = ZKPaths.makePath(baseRebalancePath, Constants.CHANGES_NODE);
        this.changesHistoryPath = ZKPaths.makePath(baseRebalancePath, Constants.CHANGES_HISTORY_NODE);
        this.tasksPath = ZKPaths.makePath(baseRebalancePath, Constants.TASKS_NODE);
        this.tasksHistoryPath = ZKPaths.makePath(baseRebalancePath, Constants.TASKS_HISTORY_NODE);
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.snManager = snManager;
        this.partitionInfoManager = partitionInfoManager;

        taskGenerator = new SimpleTaskGenerator();
        this.client = client;
        this.client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
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
        String leaderPath = ZKPaths.makePath(balancePath, Constants.DISPATCH_LEADER);
        LOG.info("leader path: {}", leaderPath);
        leaderLath = new LeaderLatch(this.client, leaderPath);
        leaderLath.addListener(new LeaderLatchListener() {
            @Override
            public void notLeader() {
                LOG.info("I'am not taskDispatch leader!");
            }

            @Override
            public void isLeader() {
                LOG.info("I'am taskDispatch leader!");
            }
        });
        treeCache = CuratorCacheFactory.getTreeCache();

        this.virtualDelay = Math.max(virtualDelay, 60);
        this.normalDelay = Math.max(normalDelay, 60);
    }

    public void start() throws Exception {
        LOG.info("begin leaderLath server!");
        leaderLath.start();

        LOG.info("changeMonitorPath: {}", changesPath);
        treeCache.addListener(changesPath, new ServerChangeListener(this));

        LOG.info("balance tasks path: {}", tasksPath);
        treeCache.addListener(tasksPath, new TaskStatusListener(this));

        // 此线程的作用就是将detailQueue队列中的磁盘变更情况更新到changeSummaryCache缓存中
        singleServer.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    dealChangeSDetail();
                } catch (InterruptedException e) {
                    LOG.error("consumer queue error!!", e);
                }
            }
        });

        scheduleExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (leaderLath.hasLeadership()) {
                        if (isLoad.get()) {
                            LOG.debug("auditTask timer changeSummaryCache: {}", changeSummaryCache);
                            for (Entry<Integer, List<DiskPartitionChangeSummary>> entry : changeSummaryCache.entrySet()) {
                                StorageRegion sn = snManager.findStorageRegionById(entry.getKey());
                                // 因为sn可能会被删除
                                if (sn != null) {
                                    syncAuditTask(entry.getKey(), entry.getValue());
                                }
                            }
                        } else {
                            LOG.info("load once change summary cache!");
                            loadCache();
                            isLoad.set(true);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("audit task schedule execute error", e);
                }
            }
        }, 3000, 3000, TimeUnit.MILLISECONDS);
    }

    /**
     * 概述：leader 节点加载changes和tasks到本地缓存
     *
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void loadCache() throws Exception {
        // 此处获得子节点名称

        List<String> snPaths = null;
        if (this.client.checkExists().forPath(changesPath) != null) {
            snPaths = this.client.getChildren().forPath(changesPath);
        }

        if (snPaths != null) {
            for (String snNode : snPaths) {
                String snPath = ZKPaths.makePath(changesPath, snNode);
                List<String> childPaths = null;
                if (this.client.checkExists().forPath(snPath) != null) {
                    childPaths = this.client.getChildren().forPath(snPath);
                }

                List<DiskPartitionChangeSummary> changeSummaries = new CopyOnWriteArrayList<>();
                if (childPaths != null) {
                    for (String childNode : childPaths) {
                        String childPath = ZKPaths.makePath(snPath, childNode);
                        byte[] data = this.client.getData().forPath(childPath);
                        DiskPartitionChangeSummary cs = JsonUtils.toObjectQuietly(data, DiskPartitionChangeSummary.class);
                        if (cs != null) {
                            changeSummaries.add(cs);
                        } else {
                            LOG.warn("find invalid change {}", childPath);
                        }
                    }
                }
                // 如果该目录下有服务变更信息，则进行服务变更信息保存
                if (!changeSummaries.isEmpty()) {
                    // 需要对changeSummary进行已时间来排序
                    Collections.sort(changeSummaries);
                    changeSummaryCache.put(Integer.parseInt(snNode), changeSummaries);
                }
            }
        }

        // 加载任务缓存
        List<String> sns = null;
        if (this.client.checkExists().forPath(tasksPath) != null) {
            sns = this.client.getChildren().forPath(tasksPath);
        }
        if (sns != null && !sns.isEmpty()) {
            for (String sn : sns) {
                String taskNode = ZKPaths.makePath(tasksPath, sn, Constants.TASK_NODE);
                if (this.client.checkExists().forPath(taskNode) != null) {
                    BalanceTaskSummary bts =
                        JsonUtils.toObjectQuietly(this.client.getData().forPath(taskNode), BalanceTaskSummary.class);
                    runTask.put(Integer.valueOf(sn), bts);
                }
            }
        }
    }

    public void dealChangeSDetail() throws InterruptedException {
        while (true) {
            DiskPartitionChangeSummary cs = detailQueue.take();
            addOneCache(cs);
        }
    }

    /**
     * @description: 将记录在队列里的变更信息更新到缓存
     */
    public void addOneCache(DiskPartitionChangeSummary cs) {
        int storageIndex = cs.getStorageIndex();
        List<DiskPartitionChangeSummary> changeSummaries = changeSummaryCache.get(storageIndex);

        if (changeSummaries == null) {
            changeSummaries = new CopyOnWriteArrayList<>();
            changeSummaryCache.put(storageIndex, changeSummaries);
        }
        if (!changeSummaries.contains(cs)) {
            LOG.info("add change summary [{}] to cache.", cs);
            changeSummaries.add(cs);
        }
        LOG.info("current storageIndex:{}, changeSummaries: {}, all changeSummaryCache: {}", storageIndex, changeSummaries,
                 changeSummaryCache);
    }

    public void syncTaskTerminal(CuratorFramework client, TreeCacheEvent event) throws Exception {
        try {
            lock.lock();
            taskTerminal(client, event);
        } finally {
            lock.unlock();
        }
    }

    public void taskTerminal(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if (leaderLath.hasLeadership()) {
            LOG.info("is leaderLath: {}", getLeaderLatch().hasLeadership());
            LOG.debug("task Dispatch event detail: {}", RebalanceUtils.convertEvent(event));

            if (event.getType() == Type.NODE_UPDATED) {
                if (event.getData() != null && event.getData().getData() != null) {
                    // 此处会检测任务是否完成
                    String eventPath = event.getData().getPath();
                    if (eventPath.substring(eventPath.lastIndexOf('/') + 1).equals(Constants.TASK_NODE)) {
                        return;
                    }
                    String parentPath = StringUtils.substring(eventPath, 0, eventPath.lastIndexOf('/'));
                    // 节点已经删除，则忽略
                    if (client.checkExists().forPath(parentPath) == null) {
                        return;
                    }

                    List<String> serverIds = client.getChildren().forPath(parentPath);

                    // 判断是否所有的节点做完任务
                    boolean finishFlag = true;
                    if (serverIds != null) {
                        if (serverIds.isEmpty()) {
                            LOG.info("task operation is not execute task, because serverIds is empty!");
                            finishFlag = false;
                        } else {
                            for (String serverId : serverIds) {
                                String nodePath = ZKPaths.makePath(parentPath, serverId);
                                TaskDetail td = JsonUtils.toObjectQuietly(client.getData().forPath(nodePath), TaskDetail.class);
                                if (td != null && td.getStatus() != DataRecover.ExecutionStatus.FINISH) {
                                    finishFlag = false;
                                    break;
                                }
                            }
                        }
                    }

                    if (finishFlag) {
                        // 所有的服务都则发布迁移规则，并清理任务
                        BalanceTaskSummary bts =
                            JsonUtils.toObjectQuietly(client.getData().forPath(parentPath), BalanceTaskSummary.class);
                        // 先更新任务状态为finish
                        updateTaskStatus(bts, TaskStatus.FINISH);
                        // 发布路由规则
                        if (bts.getTaskType() == RecoverType.VIRTUAL) {
                            LOG.debug("one virtual task finish, detail:" + RebalanceUtils.convertEvent(event));
                            String virtualRouteNode =
                                ZKPaths.makePath(virtualRoutePath, String.valueOf(bts.getStorageIndex()), bts.getId());
                            VirtualRoute route = new VirtualRoute(bts.getChangeID(), bts.getStorageIndex(), bts.getServerId(),
                                                                  bts.getInputServers().get(0), TaskVersion.V1);
                            LOG.debug("add virtual route: {}", route);
                            addRoute(virtualRouteNode, JsonUtils.toJsonBytesQuietly(route));
                            // 无效化virtualID,直到成功
                            boolean flag = false;
                            do {
                                flag = idManager.invalidVirtualId(bts.getStorageIndex(), bts.getServerId());
                            } while (!flag);
                            // 删除virtual server ID
                            LOG.debug("delete the virtual server id: {}", bts.getServerId());
                            idManager.deleteVirtualId(bts.getStorageIndex(), bts.getServerId());
                        } else if (bts.getTaskType() == RecoverType.NORMAL) {
                            LOG.debug("one normal task finish, detail:" + RebalanceUtils.convertEvent(event));
                            String normalRouteNode =
                                ZKPaths.makePath(normalRoutePath, String.valueOf(bts.getStorageIndex()), bts.getId());
                            NormalRouteV2 route =
                                new NormalRouteV2(bts.getChangeID(), bts.getStorageIndex(), bts.getServerId(),
                                                  bts.getNewSecondIds(), bts.getSecondFirstShip());
                            LOG.debug("add normal route: {}", route);
                            addRoute(normalRouteNode, JsonUtils.toJsonBytesQuietly(route));
                        }

                        List<DiskPartitionChangeSummary> changeSummaries = changeSummaryCache.get(bts.getStorageIndex());

                        // 清理zk上的变更
                        removeChange2History(bts);

                        // 清理change cache缓存
                        if (changeSummaries != null) {
                            changeSummaries.removeIf(cs -> cs.getChangeID().equals(bts.getChangeID()));
                        }
                        // 删除zk上的任务节点
                        if (delBalanceTask(bts)) {
                            // 清理task缓存
                            removeRunTask(bts.getStorageIndex());
                        }
                        LOG.info("StorageRegion:[{}],task:[{}],type:[{}] finish", bts.getStorageIndex(), bts.getChangeID(),
                                 bts.getTaskType());
                    }
                }
            }
        }

    }

    private void addRoute(String node, byte[] data) throws Exception {
        if (client.checkExists().forPath(node) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(node, data);
        }
    }

    public void fixTaskMeta(BalanceTaskSummary taskSummary) throws Exception {

        // task路径
        String parentPath = ZKPaths.makePath(tasksPath, String.valueOf(taskSummary.getStorageIndex()), Constants.TASK_NODE);
        // 节点已经删除，则忽略
        if (client.checkExists().forPath(parentPath) == null) {
            LOG.warn("task node is not exists {}", parentPath);
            runTask.remove(taskSummary.getStorageIndex());
            return;
        }
        BalanceTaskSummary bts = JsonUtils.toObjectQuietly(client.getData().forPath(parentPath), BalanceTaskSummary.class);

        // 所有的服务都则发布迁移规则，并清理任务
        if (bts.getTaskStatus().equals(TaskStatus.FINISH)) {
            if (bts.getTaskType() == RecoverType.VIRTUAL) {
                LOG.debug("one virtual task finish, detail: {}", taskSummary);
                String virtualRouteNode =
                    ZKPaths.makePath(virtualRoutePath, String.valueOf(bts.getStorageIndex()), bts.getId());
                VirtualRoute route =
                    new VirtualRoute(bts.getChangeID(), bts.getStorageIndex(), bts.getServerId(),
                                     bts.getInputServers().get(0),
                                     TaskVersion.V1);
                LOG.debug("add virtual route: {}", route);
                addRoute(virtualRouteNode, JsonUtils.toJsonBytesQuietly(route));

                // 删除virtual server ID
                LOG.debug("delete the virtual server id:" + bts.getServerId());
                idManager.deleteVirtualId(bts.getStorageIndex(), bts.getServerId());
            } else if (bts.getTaskType() == RecoverType.NORMAL) {
                LOG.debug("one normal task finish, detail: {}", taskSummary);
                String normalRouteNode =
                    ZKPaths.makePath(normalRoutePath, String.valueOf(bts.getStorageIndex()), bts.getId());

                NormalRouteV2 route =
                    new NormalRouteV2(bts.getChangeID(), bts.getStorageIndex(), bts.getServerId(), bts.getNewSecondIds(),
                                      bts.getSecondFirstShip());
                LOG.debug("add normal route:" + route);
                addRoute(normalRouteNode, JsonUtils.toJsonBytesQuietly(route));
            }
            LOG.info("storage:[{}] task [{}] type:[{}] is finish. handler the route", bts.getStorageIndex(),
                     bts.getChangeID(),
                     bts.getTaskType());
        }

        List<DiskPartitionChangeSummary> changeSummaries = changeSummaryCache.get(bts.getStorageIndex());

        // 清理zk上的变更
        removeChange2History(bts);

        // 清理change cache缓存
        if (changeSummaries != null) {
            changeSummaries.removeIf(cs -> cs.getChangeID().equals(bts.getChangeID()));
        }

        // 删除zk上的任务节点
        if (delBalanceTask(bts)) {
            // 清理task缓存
            removeRunTask(bts.getStorageIndex());
        }

    }

    /**
     * 概述：
     *
     * @param changeSummaries
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void syncAuditTask(int snIndex, List<DiskPartitionChangeSummary> changeSummaries) throws Exception {
        try {
            lock.lock();
            auditTask(snIndex, changeSummaries);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 概述：非阻塞审计任务
     *
     * @param changeSummaries
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void auditTask(int snIndex, List<DiskPartitionChangeSummary> changeSummaries) throws Exception {
        if (changeSummaries == null || changeSummaries.isEmpty()) {
            LOG.debug("snIndex:{}, changeSummaries is empty, return!!!", snIndex);
            return;
        }

        LOG.debug("audit snIndex:{},changeSummaries:{}", snIndex, changeSummaries);

        // 当前有任务在执行,则检查是否有影响该任务的change存在
        BalanceTaskSummary runningTask = runTask.get(snIndex);
        if (runningTask != null) {
            LOG.debug("this sn [{}] has running task, will check, tasks:{}", snIndex, runningTask);
            checkTask(snIndex, changeSummaries);
            return;
        }

        // 没有正在执行的任务时，优先处理虚拟迁移任务
        trimTask(changeSummaries);
        // 先检查虚拟serverID
        // 没有找到虚拟serverID迁移的任务，执行普通迁移的任务
        if (!dealVirtualTask(snIndex, changeSummaries)) {
            // String serverId = changeSummary.getChangeServer();
            dealNormalTask(snIndex, changeSummaries);
        }

    }

    /**
     * 概述：去掉无用的变更
     *
     * @param changeSummaries
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void trimTask(List<DiskPartitionChangeSummary> changeSummaries) {
        // 需要清除变更抵消。
        Map<String, List<DiskPartitionChangeSummary>> tempCsMap = Maps.newHashMap();
        // 需要清除变更抵消。
        for (DiskPartitionChangeSummary cs : changeSummaries) {
            if (tempCsMap.get(cs.getChangeServer()) == null) {
                List<DiskPartitionChangeSummary> csList = Lists.newArrayList();
                csList.add(cs);
                tempCsMap.put(cs.getChangeServer(), csList);
            } else {
                tempCsMap.get(cs.getChangeServer()).add(cs);
            }
        }
        for (Entry<String, List<DiskPartitionChangeSummary>> entry : tempCsMap.entrySet()) {
            List<DiskPartitionChangeSummary> csList = entry.getValue();
            Collections.sort(csList);
            DiskPartitionChangeSummary lastCs = csList.get(csList.size() - 1);
            for (DiskPartitionChangeSummary tmpCs : csList) {
                if (!StringUtils.equals(lastCs.getChangeID(), tmpCs.getChangeID())) {
                    // 不是最终状态的变更都删除掉
                    changeSummaries.remove(tmpCs);
                    delChangeSummaryNode(tmpCs);
                }
            }
        }
    }

    /**
     * 概述：处理remove导致的数据迁移
     *
     * @param changeSummaries
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private boolean dealNormalTask(int snIndex, List<DiskPartitionChangeSummary> changeSummaries) throws Exception {
        /*
         * 根据当时的的情况来判定，决策者如何决定，分为三种
         * 1.该SN正常，未做任何操作
         * 2.该SN正在进行virtual serverID恢复，此时分为两种，1.移除的机器为正在进行virtual ID映射的机器，2.移除的机器为其他参与者的机器
         * 3.该SN正在进行副本丢失迁移，此时会根据副本数来决定迁移是否继续。
         */
        Map<String, Integer> secondFreeMap = new HashMap<>();
        partitionInfoManager.getDiskPartitionInfoFreeSize().forEach(
            (key, value) -> {
                String second = idManager.getSecondId(key, snIndex);
                secondFreeMap.put(second, value);
            }
        );
        Map<String, String> secondFirstShip = idManager.getSecondFirstShip(snIndex);
        // 检测是否能进行数据恢复。
        for (DiskPartitionChangeSummary cs : changeSummaries) {
            if (cs.getChangeType().equals(ChangeType.REMOVE)) {
                List<String> alivePartitionIds = this.partitionInfoManager.getCurrentPartitionIds();
                List<String> joinerPartitionIds = cs.getCurrentPartitionIds();
                LOG.debug("alivePartitionIds: {}", alivePartitionIds);
                LOG.debug("joinerPartitionIds: {}", joinerPartitionIds);
                LOG.debug("further to filter dead server...");
                List<String> aliveSecondIDs = alivePartitionIds.stream()
                                                               .map((x) -> idManager.getSecondId(x, cs.getStorageIndex()))
                                                               .collect(Collectors.toList());
                List<String> joinerSecondIDs = joinerPartitionIds.stream()
                                                                 .map((partitionId) -> idManager
                                                                     .getSecondId(partitionId, cs.getStorageIndex()))
                                                                 .collect(Collectors.toList());

                // 挂掉的机器不能做生存者和参与者，此处进行再次过滤，防止其他情况
                if (joinerSecondIDs.contains(cs.getChangeServer())) {
                    joinerSecondIDs.remove(cs.getChangeServer());
                }

                boolean canRecover = isCanRecover(cs, joinerSecondIDs, aliveSecondIDs, idManager, cs.getStorageIndex());
                if (canRecover) {
                    // 构建任务
                    BalanceTaskSummary taskSummary = taskGenerator
                        .genBalanceTask(cs.getChangeID(), cs.getStorageIndex(), cs.getChangePartitionId(),
                                        cs.getChangeServer(),
                                        aliveSecondIDs, joinerSecondIDs, secondFreeMap, secondFirstShip, normalDelay);
                    // 发布任务
                    dispatchTask(taskSummary);
                    // 加入正在执行的任务的缓存中
                    setRunTask(taskSummary.getStorageIndex(), taskSummary);
                    LOG.info("release storageRegion:[{}], task:[{}], type:[{}], status:[{}] delaytime:[{}]s",
                             cs.getStorageIndex(), cs.getChangeID(), cs.getChangeType(), taskSummary.getTaskStatus(),
                             normalDelay);
                }
            }
        }
        return true;
    }

    private List<String> getAliveServices() {
        return serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup())
                             .stream()
                             .map(Service::getServiceId).collect(Collectors.toList());
    }

    private boolean isCanRecover(DiskPartitionChangeSummary
                                     cs, List<String> joinerSecondIDs, List<String> aliveSecondIDs,
                                 IDSManager idManager, int storageIndex) {
        if (aliveSecondIDs.contains(cs.getChangeServer())) {
            LOG.warn("change:[{}] No need to execute,the reason is changeServer alive!  change:[{}] aliveServers:{}",
                     cs.getChangeID(), cs.getChangeServer(), aliveSecondIDs);
            return false;
        }
        int replicas = snManager.findStorageRegionById(cs.getStorageIndex()).getReplicateNum();
        // 检查参与者是否都存活
        for (String joiner : joinerSecondIDs) {
            if (!aliveSecondIDs.contains(joiner)) {
                LOG.warn("change:[{}] No need to execute,the reason is joiner dead!  joiners:{} aliveServers:{}",
                         cs.getChangeID(), joinerSecondIDs, aliveSecondIDs);
                return false;
            }
        }
        // 检查目前存活的服务，是否满足副本数
        if (aliveSecondIDs.size() < replicas) {
            LOG.warn("change:[{}] No need to execute,the reason is replicas num > alive num!  replicas:[{}] aliveServers:{}",
                     cs.getChangeID(), replicas, aliveSecondIDs);
            return false;
        }

        // 判断当前的磁盘分区是否是同一个节点的，如果是，不做恢复操作
        String serverId = idManager.getFirstSever();
        Collection<String> secondIds = idManager.getSecondIds(serverId, storageIndex);

        if (secondIds.containsAll(aliveSecondIDs)) {
            LOG.warn(
                "change:[{}] No need to execute,the reason is only one server!  "
                    + "firstServer: [{}], partitionFirstShip:{} aliveServers:{}",
                cs.getChangeID(), serverId, secondIds, aliveSecondIDs);
            return false;
        }

        return true;
    }

    /**
     * 概述：处理add导致的virtual server id数据迁移
     *
     * @param changeSummaries
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private boolean dealVirtualTask(int snIndex, List<DiskPartitionChangeSummary> changeSummaries) throws Exception {
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
        LOG.info("deal virtual task, snIndex:{}", snIndex);
        boolean addFlag = false;
        for (DiskPartitionChangeSummary changeSummary : changeSummaries) {
            if (changeSummary.getChangeType().equals(ChangeType.ADD)) { // 找到第一个ADD
                String changeID = changeSummary.getChangeID();
                int storageIndex = changeSummary.getStorageIndex();
                List<String> currentFirstIDs = getAliveServices();
                List<String> virtualServerIds = idManager.listValidVirtualIds(changeSummary.getStorageIndex());
                String virtualServersPath = idManager.getVirtualIdContainerPath();
                if (virtualServerIds != null && !virtualServerIds.isEmpty()) {
                    Collections.sort(virtualServerIds);
                    for (String virtualID : virtualServerIds) {
                        // 获取使用该serverID的参与者的firstID。
                        String vidPath = ZKPaths.makePath(virtualServersPath, String.valueOf(storageIndex), virtualID);
                        List<String> participators = client.getChildren().forPath(vidPath);
                        // 如果当前存活的firstID包括该 virtualID的参与者，那么
                        List<String> selectIds = selectAvailableIDs(currentFirstIDs, participators);

                        if (selectIds != null && !selectIds.isEmpty()) {
                            // 需要寻找一个可以恢复的虚拟serverID，此处选择新来的或者没参与过的
                            // 构建任务需要使用2级serverid
                            String selectSecondID = idManager.getSecondId(changeSummary.getChangePartitionId(), storageIndex);

                            Collection<String> secondParticipators = null;
                            List<String> aliveServices = getAliveServices();

                            // 选择一个活着的可用的参与者
                            for (String participator : participators) {
                                if (aliveServices.contains(participator)) {
                                    secondParticipators = idManager.getSecondIds(participator, storageIndex);
                                    break;
                                }
                            }

                            if (secondParticipators == null || secondParticipators.isEmpty()) {
                                LOG.error("select participator for virtual recover error!!");
                                return false;
                            }

                            // 构造任务
                            BalanceTaskSummary taskSummary = taskGenerator
                                .genVirtualTask(changeID, storageIndex, changeSummary.getChangePartitionId(), virtualID,
                                                Lists.newArrayList(selectSecondID), (List<String>) secondParticipators,
                                                partitionInfoManager.getDiskPartitionInfoFreeSize(), virtualDelay);
                            // 只在任务节点上创建任务，taskOperator会监听，去执行任务

                            dispatchTask(taskSummary);

                            // 添加virtual task 成功
                            addFlag = true;
                            setRunTask(changeSummary.getStorageIndex(), taskSummary);

                            // 虚拟serverID置为无效
                            // 无效化virtualID,直到成功
                            boolean flag = false;
                            do {
                                flag = idManager.invalidVirtualId(taskSummary.getStorageIndex(), taskSummary.getServerId());
                            } while (!flag);
                            // 虚拟serverID迁移完成，会清理缓存和zk上的任务
                            break;
                        } else {
                            LOG.info(
                                "This change does not need to participate in virtual migration"
                                    + " (use virtual serverID, or have participated in virtual serverID recovery): {}",
                                changeSummary.getChangeID());
                            changeSummaries.remove(changeSummary);
                            delChangeSummaryNode(changeSummary);

                        }
                    }
                } else {
                    // 没有使用virtual id ，则不需要进行数据迁移
                    LOG.info("none virtual server id to recover for change: {}", changeSummary);
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

    private void checkTask(int snIndex, List<DiskPartitionChangeSummary> changeSummaries) throws Exception {
        // 获取当前任务信息
        BalanceTaskSummary currentTask = runTask.get(snIndex);
        String runChangeID = currentTask.getChangeID();

        Map<String, List<DiskPartitionChangeSummary>> tempCsMap = Maps.newHashMap();
        // trim change cache 清除变更抵消,查看最终的状态即可
        for (DiskPartitionChangeSummary cs : changeSummaries) {
            if (!StringUtils.equals(cs.getChangeID(), runChangeID)) {
                if (tempCsMap.get(cs.getChangeServer()) == null) {
                    List<DiskPartitionChangeSummary> csList = Lists.newArrayList();
                    csList.add(cs);
                    tempCsMap.put(cs.getChangeServer(), csList);
                } else {
                    tempCsMap.get(cs.getChangeServer()).add(cs);
                }
            }
        }

        for (Entry<String, List<DiskPartitionChangeSummary>> entry : tempCsMap.entrySet()) {
            List<DiskPartitionChangeSummary> csList = entry.getValue();
            Collections.sort(csList);
            DiskPartitionChangeSummary lastCs = csList.get(csList.size() - 1);
            for (DiskPartitionChangeSummary tmpCs : csList) {
                if (!StringUtils.equals(lastCs.getChangeID(), tmpCs.getChangeID())) {
                    //不是最终状态的变更都删除掉
                    changeSummaries.remove(tmpCs);
                    delChangeSummaryNode(tmpCs);
                }
            }
        }

        LOG.debug("check change summaries:{}", changeSummaries);
        // 查找影响当前任务的变更
        if (changeSummaries.size() > 1) {
            String changeID = currentTask.getChangeID();
            // 找到正在执行的变更
            Optional<DiskPartitionChangeSummary> runChangeOpt =
                changeSummaries.stream().filter(x -> x.getChangeID().equals(changeID)).findFirst();
            if (!runChangeOpt.isPresent()) {
                LOG.error("rebalance metadata is error: {} try fix it !!", currentTask);
                MailWorker.newBuilder(EmailPool.getInstance().getProgramInfo())
                          .setMessage("rebalance metadata is error:" + currentTask);
                // 尝试修复下
                fixTaskMeta(currentTask);
                return;
            }
            DiskPartitionChangeSummary runChangeSummary = runChangeOpt.get();

            LOG.debug("check running change: {}", runChangeSummary);
            for (DiskPartitionChangeSummary cs : changeSummaries) {
                if (!cs.getChangeID().equals(runChangeSummary.getChangeID())) { // 与正在执行的变更不同
                    if (runChangeSummary.getChangeType().equals(ChangeType.ADD)) { // 正在执行虚拟迁移任务
                        if (cs.getChangeType().equals(ChangeType.REMOVE)) { // 虚拟迁移时，出现问题
                            // 虚拟迁移时，接收者出现问题
                            if (cs.getChangeServer().equals(currentTask.getInputServers().get(0))) {
                                LOG.warn("running virtual task,receiver has fault,server id:{}",
                                         currentTask.getInputServers().get(0));
                                List<String> aliveServices = getAliveServices();
                                String otherFirstID =
                                    idManager.getFirstId(currentTask.getInputServers().get(0), currentTask.getStorageIndex());
                                if (!aliveServices.contains(otherFirstID) && countdownTime(currentTask)) {
                                    if (!currentTask.getTaskStatus().equals(TaskStatus.CANCEL)) {
                                        updateTaskStatus(currentTask, TaskStatus.CANCEL);
                                        changeSummaries.remove(runChangeSummary);
                                        delChangeSummaryNode(runChangeSummary);

                                        removeRunTask(currentTask.getStorageIndex());
                                        delBalanceTask(currentTask);
                                        // 将virtual serverID 标为可用
                                        idManager.validVirtualId(currentTask.getStorageIndex(), currentTask.getServerId());
                                        LOG.warn("storage:[{}], task:[{}] type:[{}] cancle", snIndex, changeID,
                                                 runChangeSummary.getChangeType());
                                    }
                                }

                            } else {
                                List<String> joiners = currentTask.getOutputServers();
                                if (joiners.contains(cs.getChangeServer())) { // 参与者挂掉
                                    LOG.warn("running virtual task,joiner has fault,server id:{}",
                                             currentTask.getInputServers().get(0));
                                    if (countdownTime(currentTask)) {
                                        List<String> aliveServices = getAliveServices();
                                        String otherFirstID = idManager.getFirstId(currentTask.getOutputServers().get(0),
                                                                                   runChangeSummary.getStorageIndex());
                                        if (!aliveServices.contains(otherFirstID)) {
                                            LOG.warn("joiner is not aliver,reselct route role");
                                            // 重新选择
                                            String virtualServersPath = idManager.getVirtualIdContainerPath();
                                            String virtualServerNodePath =
                                                virtualServersPath + Constants.SEPARATOR + currentTask.getStorageIndex()
                                                    + Constants.SEPARATOR + currentTask.getServerId();
                                            List<String> participators = client.getChildren().forPath(virtualServerNodePath);
                                            String secondParticipator = null;
                                            for (String participator : participators) {
                                                if (aliveServices.contains(participator)) {
                                                    if (participator.equals(idManager.getFirstSever())) {
                                                        secondParticipator = idManager
                                                            .getSecondId(runChangeSummary.getChangePartitionId(),
                                                                         currentTask.getStorageIndex());
                                                    } else {
                                                        secondParticipator = idManager.getSecondId(cs.getChangePartitionId(),
                                                                                                   currentTask
                                                                                                       .getStorageIndex());
                                                    }
                                                    break;
                                                }
                                            }
                                            if (secondParticipator != null) { // 选择成功
                                                // 删除以前的task
                                                delBalanceTask(currentTask);
                                                currentTask.setOutputServers(Lists.newArrayList(secondParticipator));
                                                currentTask.setTaskStatus(TaskStatus.INIT);
                                                // 重新分发task
                                                dispatchTask(currentTask);
                                                LOG.warn("storage:[{}], task:[{}] type:[{}] restart", snIndex, changeID,
                                                         runChangeSummary.getChangeType());
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    } else if (runChangeSummary.getChangeType().equals(ChangeType.REMOVE)) { // 正在执行普通迁移任务
                        if (cs.getChangeType().equals(ChangeType.ADD)) {
                            // 正在执行的任务为remove恢复，检测到ADD事件，并且是同一个serverID
                            if (cs.getChangeServer().equals(runChangeSummary.getChangeServer())) {
                                LOG.debug("check the same change with running task...");
                                String taskPath = ZKPaths
                                    .makePath(tasksPath, String.valueOf(runChangeSummary.getStorageIndex()),
                                              Constants.TASK_NODE);

                                // 任务进度小于指定进度，则终止任务
                                double process = monitor.getTaskProgress(client, taskPath);

                                if (process < DEFAULT_PROCESS) {
                                    if (!currentTask.getTaskStatus().equals(TaskStatus.CANCEL)) {
                                        updateTaskStatus(currentTask, TaskStatus.CANCEL);
                                        changeSummaries.remove(runChangeSummary);
                                        delChangeSummaryNode(runChangeSummary);
                                        removeRunTask(currentTask.getStorageIndex());
                                        delBalanceTask(currentTask);
                                        LOG.warn("storage:[{}], task:[{}] type:[{}] cancle with process {}", snIndex,
                                                 changeID,
                                                 runChangeSummary.getChangeType(), process);
                                    }
                                    break;
                                }

                            } else { // 不为同一个serverID
                                // 如果任务暂停，查看回来的是否为曾经的参与者
                                if (currentTask.getTaskStatus().equals(TaskStatus.PAUSE)) {
                                    List<String> alivePartitionIds = this.partitionInfoManager.getCurrentPartitionIds();
                                    List<String> aliveSecondIDs =
                                        alivePartitionIds.stream().map((x) -> idManager.getSecondId(x, cs.getStorageIndex()))
                                                         .collect(Collectors.toList());
                                    // 参与者和接收者都存活
                                    if (aliveSecondIDs.containsAll(currentTask.getOutputServers())
                                        && aliveSecondIDs.containsAll(currentTask.getInputServers())) {
                                        updateTaskStatus(currentTask, TaskStatus.RUNNING);
                                        LOG.warn("storage:[{}], task:[{}] type:[{}] pause to runing", snIndex, changeID,
                                                 runChangeSummary.getChangeType());
                                    }
                                }
                            }
                        } else if (cs.getChangeType().equals(ChangeType.REMOVE)) {
                            // 有可能是参与者挂掉，参与者包括接收者和发送者
                            // 参与者停止恢复 停止恢复必须查看是否有
                            String secondID = cs.getChangeServer();
                            List<String> joiners = currentTask.getOutputServers();
                            List<String> receivers = currentTask.getInputServers();

                            // 参与者出现问题 或 既是参与者又是接收者
                            if (joiners.contains(secondID) || receivers.contains(secondID)) {
                                if (!TaskStatus.PAUSE.equals(currentTask.getTaskStatus())) {
                                    updateTaskStatus(currentTask, TaskStatus.PAUSE);
                                    LOG.warn("storage:[{}], task:[{}] type:[{}] {} to pause", snIndex, changeID,
                                             currentTask.getTaskStatus(), runChangeSummary.getChangeType());
                                }
                                if (countdownTime(currentTask)) {
                                    LOG.warn("storage:[{}], task:[{}] type:[{}] {} to cancel", snIndex, changeID,
                                             currentTask.getTaskType(), currentTask.getTaskStatus(),
                                             runChangeSummary.getChangeType());
                                    updateTaskStatus(currentTask, TaskStatus.CANCEL);
                                    removeRunTask(currentTask.getStorageIndex());
                                    delBalanceTask(currentTask);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            LOG.debug("one change result in running, changeid: {}", runChangeID);
        }
    }

    /**
     * 延迟任务倒计数
     *
     * @param currentTask
     *
     * @return
     */
    public boolean countdownTime(BalanceTaskSummary currentTask) {
        boolean run;
        long delay = currentTask.getDelayTime();
        int interval = currentTask.getInterval();
        if (interval == -1) {
            currentTask.setInterval(DEFAULT_INTERVAL);
            interval = currentTask.getInterval();
        }
        if (interval == DEFAULT_INTERVAL) {
            LOG.warn("current run task have trouble !"
                         + " will wait it for {} times, changeID:[{}], storageRegion:[{}],Type:[{}]",
                     DEFAULT_INTERVAL, currentTask.getChangeID(), currentTask.getStorageIndex(), currentTask.getTaskType());
        }
        if (delay <= 600) {
            run = true;
        } else {
            currentTask.setInterval(currentTask.getInterval() - 1);
            run = currentTask.getInterval() == 0;
        }
        if (run) {
            LOG.warn("current run task have trouble !"
                         + " now run it , changeID:[{}], storageRegion:[{}],Type:[{}]",
                     currentTask.getStorageIndex(), currentTask.getTaskType());
        }
        return run;
    }

    public void delChangeSummaryNode(DiskPartitionChangeSummary summary) {
        LOG.info("delete change summary: {}", summary);
        String path = ZKPaths.makePath(changesPath, String.valueOf(summary.getStorageIndex()), summary.getChangeID());
        String historyPath =
            ZKPaths.makePath(changesHistoryPath, String.valueOf(summary.getStorageIndex()), summary.getChangeID());
        try {
            byte[] data = client.getData().forPath(path);
            client.delete().guaranteed().forPath(path);
            client.create().creatingParentsIfNeeded().forPath(historyPath, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 删除balance task, 本质就是将其信息移至history节点
     */
    public boolean delBalanceTask(BalanceTaskSummary task) {
        LOG.info("delete task: {}", task);
        String taskNode = ZKPaths.makePath(tasksPath, String.valueOf(task.getStorageIndex()), Constants.TASK_NODE);
        String taskHistory = ZKPaths.makePath(tasksHistoryPath, String.valueOf(task.getStorageIndex()), task.getChangeID());
        try {
            byte[] data = client.getData().forPath(taskNode);
            if (client.checkExists().forPath(taskNode) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(taskNode);
            }
            client.create().creatingParentsIfNeeded().forPath(taskHistory, data);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void updateTaskStatus(BalanceTaskSummary task, TaskStatus status) throws Exception {
        task.setTaskStatus(status);
        String taskNode = ZKPaths.makePath(tasksPath, String.valueOf(task.getStorageIndex()), Constants.TASK_NODE);
        client.setData().forPath(taskNode, JsonUtils.toJsonBytesQuietly(task));
    }

    /**
     * 概述：在任务节点上创建任务
     *
     * @param taskSummary
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean dispatchTask(BalanceTaskSummary taskSummary) throws Exception {
        int storageIndex = taskSummary.getStorageIndex();
        // 设置唯一任务UUID
        taskSummary.setId(UUID.randomUUID().toString());

        String jsonStr = JsonUtils.toJsonStringQuietly(taskSummary);
        LOG.debug("dispatch task:{}", jsonStr);
        // 创建任务
        String taskNode = ZKPaths.makePath(tasksPath, String.valueOf(storageIndex), Constants.TASK_NODE);
        if (client.checkExists().forPath(taskNode) == null) {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(taskNode, jsonStr.getBytes(StandardCharsets.UTF_8));
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

    public BlockingQueue<DiskPartitionChangeSummary> getDetailQueue() {
        return detailQueue;
    }

    public String getVirtualRoutePath() {
        return virtualRoutePath;
    }

    public String getNormalRoutePath() {
        return normalRoutePath;
    }

    public String getChangesPath() {
        return changesPath;
    }

    public Map<Integer, List<DiskPartitionChangeSummary>> getSummaryCache() {
        return changeSummaryCache;
    }

    public IDSManager getServerIDManager() {
        return idManager;
    }

    public void setRunTask(int storageIndex, BalanceTaskSummary task) {
        runTask.put(storageIndex, task);
    }

    public void removeRunTask(int storageIndex) {
        runTask.remove(storageIndex);
    }

    public void removeChange2History(BalanceTaskSummary bts) {
        String changePath = ZKPaths.makePath(changesPath, String.valueOf(bts.getStorageIndex()), bts.getChangeID());
        LOG.info("delete change path: {}", changePath);
        String changeHistoryPath =
            ZKPaths.makePath(changesHistoryPath, String.valueOf(bts.getStorageIndex()), bts.getChangeID());
        try {
            byte[] data = client.getData().forPath(changePath);
            if (client.checkExists().forPath(changePath) != null) {
                client.delete().forPath(changePath);
            }
            client.create().creatingParentsIfNeeded().forPath(changeHistoryPath, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        leaderLath.close();
        singleServer.shutdown();
        treeCache.cancelListener(changesPath);
        treeCache.cancelListener(tasksPath);
    }
}
