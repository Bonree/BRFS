package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.BalanceTaskGenerator;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.identification.impl.ZookeeperServerIdGen;
import com.google.common.base.Preconditions;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午4:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此处来进行任务分配，任务核心控制
 ******************************************************************************/
public class TaskDispatcher implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);

    private CuratorClient curatorClient;

    private LeaderLatch leaderLath;

    private CuratorTreeCache treeCache;

    private ZookeeperServerIdGen identification;

    private TaskMonitor monitor;

    private final String basePath;

    private final BalanceTaskGenerator taskGenerator;

    private final AtomicBoolean isLoad = new AtomicBoolean(true);

    // 此处为任务缓存，只有身为leader的server才会进行数据缓存
    private Map<Integer, List<ChangeSummary>> cacheSummaryCache = new ConcurrentHashMap<Integer, List<ChangeSummary>>();

    // 为了能够有序的处理变更，需要将变更添加到队列中
    private ArrayBlockingQueue<ChangeDetail> detailQueue = new ArrayBlockingQueue<>(256);

    class ChangeDetail {

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

    /*
     * 监听Server变更，以便生成任务
     */
    class ServerChangeListener extends AbstractTreeCacheListener {

        public ServerChangeListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            LOG.info("leaderLath:" + leaderLath.hasLeadership());
            LOG.info("server change event detail:" + event.getType());
            if (leaderLath.hasLeadership()) {
                // 检查event是否有数据
                if (!isRemovedNode(event)) { // 不是remove的时间，则需要处理
                    if (event.getData() != null && event.getData().getData() != null) {

                        // 需要进行检查，在切换leader的时候，变更记录需要加载进来。
                        if (isLoad.get()) {
                            // 此处加载缓存
                            LOG.info("load all");
                            TaskDispatcher.this.loadCache(client, event);
                            isLoad.set(false);
                        }
                        ChangeDetail detail = new ChangeDetail(client, event);
                        // 将变更细节添加到队列即可
                        detailQueue.put(detail);
                    } else {
                        LOG.info("ignore the change:" + event);
                    }
                }
            }
        }

    }

    private boolean isRemovedNode(TreeCacheEvent event) {
        if (event.getType() == Type.NODE_REMOVED) {
            return true;
        }
        return false;
    }

    private boolean isUpdatedNode(TreeCacheEvent event) {
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

                List<ChangeSummary> changeSummaries = new ArrayList<ChangeSummary>();
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

    /*
     * 监听任务是否完成，以便进行通知
     */
    class TaskDispachListener extends AbstractTreeCacheListener {

        public TaskDispachListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (leaderLath.hasLeadership()) {
                LOG.info("leaderLath:" + leaderLath.hasLeadership());
                LOG.info("task Dispatch event detail:" + event.getType());
                if (isUpdatedNode(event)) {
                    if (event.getData() != null && event.getData().getData() != null) {
                        // 此处会检测任务是否完成
                        String eventPath = event.getData().getPath();
                        String parentPath = StringUtils.substring(eventPath, 0, eventPath.lastIndexOf('/'));
                        BalanceTaskSummary bts = JSON.parseObject(client.getData().forPath(parentPath), BalanceTaskSummary.class);
                        List<String> serverIds = client.getChildren().forPath(parentPath);
                        boolean finishFlag = true;
                        if (serverIds != null && serverIds.isEmpty()) {
                            for (String serverId : serverIds) {
                                String nodePath = parentPath + Constants.SEPARATOR + serverId;
                                TaskDetail td = JSON.parseObject(client.getData().forPath(nodePath), TaskDetail.class);
                                if (td.getStatus() != DataRecover.FINISH_STAGE) {
                                    finishFlag = false;
                                    break;
                                }
                            }
                        }
                        if (finishFlag) {// 所有的服务都则发布迁移规则，并清理任务
                            String roleNode = Constants.ROLES_NODE + Constants.SEPARATOR + bts.getStorageIndex() + Constants.SEPARATOR + Constants.ROLE_NODE;
                            client.create().creatingParentsIfNeeded().forPath(roleNode, client.getData().forPath(parentPath));
                            // 清理变更
                            List<ChangeSummary> changeSummaries = cacheSummaryCache.get(bts.getStorageIndex());
                            ChangeSummary cs = changeSummaries.get(0);
                            String changePath = Constants.PATH_CHANGES + Constants.SEPARATOR + cs.getStorageIndex() + Constants.SEPARATOR + cs.getCreateTime();
                            client.delete().forPath(changePath);
                            changeSummaries.remove(0);
                            // 删除任务
                            client.delete().deletingChildrenIfNeeded().forPath(parentPath);
                            // 重新审计
                            auditTask(changeSummaries);
                        }
                    }
                }
            }
        }

    }

    public TaskDispatcher(String zkUrl, String basePath, ZookeeperServerIdGen identification) {
        this.basePath = BrStringUtils.trimBasePath(Preconditions.checkNotNull(basePath, "basePath is not null!"));
        this.identification = identification;
        taskGenerator = new SimpleTaskGenerator();

        curatorClient = CuratorClient.getClientInstance(Preconditions.checkNotNull(zkUrl, "zkUrk is not null!"));
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

        String leaderPath = this.basePath + Constants.SEPARATOR + Constants.LEADER_NODE;
        LOG.info("leader path:" + leaderPath);
        leaderLath = new LeaderLatch(curatorClient.getInnerClient(), leaderPath);
        treeCache = CuratorCacheFactory.getTreeCache();
    }

    public void dealChangeSDetail() throws InterruptedException {
        ChangeDetail cd = null;
        while (true) {
            cd = detailQueue.take();
            List<ChangeSummary> changeSummaries = addOneCache(cd.getClient(), cd.getEvent());
            auditTask(changeSummaries);
        }

    }

    public List<ChangeSummary> addOneCache(CuratorFramework client, TreeCacheEvent event) {
        List<ChangeSummary> changeSummaries = null;
        if (event.getData().getData() != null) {
            ChangeSummary changeSummary = JSON.parseObject(event.getData().getData(), ChangeSummary.class);
            int storageIndex = changeSummary.getStorageIndex();
            changeSummaries = cacheSummaryCache.get(storageIndex);

            if (changeSummaries == null) {
                changeSummaries = new ArrayList<ChangeSummary>();
                cacheSummaryCache.put(storageIndex, changeSummaries);
            }
            if (!changeSummaries.contains(changeSummary)) {
                changeSummaries.add(changeSummary);
            }
            LOG.info("changeSummaries:" + changeSummaries);
        }

        return changeSummaries;
    }

    public void auditTask(List<ChangeSummary> changeSummaries) {
        if (changeSummaries != null && !changeSummaries.isEmpty()) {
            ChangeSummary changeSummary = changeSummaries.get(0); // 获取第一个任务
            String serverId = changeSummary.getChangeServer();
            if (changeSummary.getChangeType() == ChangeType.ADD) { // 判断该次变更所产生的任务类型
                /*
                 * 第一个任务为添加服务，若是新来的，判断需要进行虚拟ServerID迁移，
                 * 若是旧的回归，因为是第一个任务变更，那只能说明旧服务的数据已经迁移完成，
                 * 此时也只是需要判断是否需要进行虚拟ServerID迁移
                 */
                List<String> virtualServerIds = identification.listVirtualIdentification();
                if (virtualServerIds != null && !virtualServerIds.isEmpty()) {// 说明目前已经有了virtual SID，需要进行虚拟SID迁移
                    Collections.sort(virtualServerIds);
                    String needRecoverId = virtualServerIds.get(0);
                    // 构造任务
                    BalanceTaskSummary taskSummary = taskGenerator.genVirtualTask(needRecoverId, changeSummary);

                    dispatchTask(taskSummary);

                    boolean flag = false;
                    do {
                        flag = identification.invalidVirtualIden(needRecoverId);
                    } while (!flag);
                }

            } else if (changeSummary.getChangeType() == ChangeType.REMOVE) {
                /*
                 * 根据当时的的情况来判定，决策者如何决定，分为三种
                 * 1.该SN正常，未做任何操作
                 * 2.该SN正在进行virtual serverID恢复，此时分为两种，1.移除的机器为正在进行virtual ID映射的机器，2.移除的机器为其他参与者的机器
                 * 3.该SN正在进行副本丢失迁移，此时会根据副本数来决定迁移是否继续。
                 */
                for (int i = 1; i < changeSummaries.size(); i++) {
                    ChangeSummary tmp = changeSummaries.get(i);
                    String tempServerId = tmp.getChangeServer();
                    if (StringUtils.equals(serverId, tempServerId)) {
                        if (tmp.getChangeType() == ChangeType.ADD) {
                            BalanceTaskSummary taskSummary = taskGenerator.genBalanceTask(changeSummary);
                            if (monitor.getTaskProgress(taskSummary) < 0.6) { // TODO 0.6暂时填充
                                cancelTask(taskSummary); // TODO 此处需要同步,为了一致性，不能是简单的修改任务状态
                                auditTask(changeSummaries);
                            }
                        }
                    }

                }
                BalanceTaskSummary taskSummary = taskGenerator.genBalanceTask(changeSummary);
                dispatchTask(taskSummary);
            }
        }

    }

    public ServerInfo getServerInfofromCache(String serverId) {
        return new ServerInfo(); // TODO 此处需要实现
    }

    public void start() throws Exception {
        LOG.info("launch TaskDispatcher!");
        curatorClient.blockUntilConnected();
        LOG.info("begin leaderLath server!");
        leaderLath.start();

        String changeMonitorPath = basePath + Constants.SEPARATOR + Constants.CHANGE_NODE;
        LOG.info("changeMonitorPath:" + changeMonitorPath);
        treeCache.addListener(changeMonitorPath, new ServerChangeListener("server_change"));
        treeCache.startPathCache(changeMonitorPath);

        String taskMonitorPath = basePath + Constants.SEPARATOR + Constants.TASKS_NODE;
        LOG.info("taskMonitorPath:" + taskMonitorPath);
        treeCache.addListener(taskMonitorPath, new TaskDispachListener("task_dispatch"));
        treeCache.startPathCache(taskMonitorPath);
    }

    public boolean dispatchTask(BalanceTaskSummary taskSummary) {
        int storageIndex = taskSummary.getStorageIndex();
        String jsonStr = JSON.toJSONString(taskSummary);
        // 创建任务
        String taskNode = Constants.PATH_TASKS + Constants.SEPARATOR + storageIndex + Constants.SEPARATOR + Constants.TASK_NODE;
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
        String taskNode = Constants.PATH_TASKS + Constants.SEPARATOR + storageIndex + Constants.SEPARATOR + serverId;
        String jsonStr = JSON.toJSONString(taskSummary);
        if (!curatorClient.checkExists(taskNode)) {
            curatorClient.createPersistent(taskNode, true, jsonStr.getBytes());
        }
        return true;
    }

    public void cleanChangeSummaryFromCache(List<ChangeSummary> changeSummaries, ChangeSummary c1) {
        // 移除这两个server变更
        changeSummaries.remove(c1);
    }

    public void cleanChangeSummaryFromZk(ChangeSummary c1) { // TODO

    }

    @Override
    public void close() throws IOException {
        if (leaderLath != null) {
            leaderLath.close();
        }

        if (curatorClient != null) {
            curatorClient.close();
        }
    }

    public static void main(String[] args) throws Exception {
        CuratorCacheFactory.init(Constants.zkUrl);
        ZookeeperServerIdGen identification = null;  // TODO
        TaskDispatcher td = new TaskDispatcher(Constants.zkUrl, Constants.BASE_PATH, identification);
        td.start();
        Thread.sleep(Long.MAX_VALUE);
        td.close();

    }

}
