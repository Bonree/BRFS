package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.identification.impl.ZookeeperIdentification;
import com.google.common.base.Preconditions;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午4:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此处来进行任务分配，任务核心控制
 ******************************************************************************/
public class TaskDispatch implements Closeable {

    private CuratorClient client;

    private LeaderLatch leaderLath;

    private CuratorTreeCache treeCache;

    private ZookeeperIdentification identification;

    private TaskMonitor monitor;

    private final String zkUrl;

    private final String basePath;

    private final AtomicBoolean isLoad = new AtomicBoolean(true);

    // 此处为任务缓存，只有身为leader的server才会进行数据缓存
    private Map<Integer, List<ChangeSummary>> cacheSummaryCache = new ConcurrentHashMap<Integer, List<ChangeSummary>>();

    // 用于处理变更任务
    class TaskDispatchListener extends AbstractTreeCacheListener {

        public TaskDispatchListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            System.out.println(leaderLath.hasLeadership());
            if (leaderLath.hasLeadership()) { // TODO 加载需要优化
                // 检查event是否有数据
                if (event.getData() != null && event.getData().getData() != null) {
                    // 需要进行检查，在切换leader的时候，变更记录需要加载进来。
                    List<ChangeSummary> changeSummarys = null;
                    if (isLoad.get()) {
                        // 此处加载缓存
                        System.out.println("load all");
                        loadCache(client, event);
                        isLoad.set(false);
                    }
                    changeSummarys = addOneCache(client, event);

                    if (changeSummarys != null) {
                        auditTask(changeSummarys);
                    }
                } else {
                    System.out.println("ignore the change:" + event);
                }
            }
        }

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

                    List<ChangeSummary> changeSummarys = new ArrayList<ChangeSummary>();
                    if (childPaths != null) {
                        for (String childNode : childPaths) {
                            String childPath = snPath + Constants.SEPARATOR + childNode;
                            byte[] data = client.getData().forPath(childPath);
                            ChangeSummary cs = JSON.parseObject(data, ChangeSummary.class);
                            changeSummarys.add(cs);
                        }
                    }
                    // 如果该目录下有服务变更信息，则进行服务变更信息保存
                    if (changeSummarys.size() > 0) {
                        // 需要对changeSummary进行已时间来排序
                        Collections.sort(changeSummarys);
                        cacheSummaryCache.put(changeSummarys.get(0).getStorageIndex(), changeSummarys);
                    }
                }
            }
        }

    }

    public List<ChangeSummary> addOneCache(CuratorFramework client, TreeCacheEvent event) {
        List<ChangeSummary> changeSummarys = null;
        if (event.getData().getData() != null) {
            ChangeSummary changeSummary = JSON.parseObject(event.getData().getData(), ChangeSummary.class);
            int storageIndex = changeSummary.getStorageIndex();
            changeSummarys = cacheSummaryCache.get(storageIndex);

            if (changeSummarys == null) {
                changeSummarys = new ArrayList<ChangeSummary>();
                cacheSummaryCache.put(storageIndex, changeSummarys);
            }
            if (!changeSummarys.contains(changeSummary)) {
                changeSummarys.add(changeSummary);
            }
            System.out.println(changeSummarys);
        }

        return changeSummarys;
    }

    public void auditTask(List<ChangeSummary> changeSummarys) {
        if (changeSummarys.size() > 0) {
            ChangeSummary changeSummary = changeSummarys.get(0); // 获取第一个任务
            String serverId = changeSummary.getChangeServer();
            if (changeSummary.getChangeType() == ChangeType.ADD) { // 判断该次变更所产生的任务类型
                /*
                 * 根据当时的情况来判定作何操作。分为2种
                 * 1.机器没有做任何操作，新加机器可能触发以下两种操作，1.是否需要进行虚拟ID迁移；
                 * 2.老机器回来，1.是否进行虚拟ID迁移，对于正在进行或将来要进行的任务进行处理；
                 */
                /*
                 * 第一个任务为添加服务，若是新来的，判断需要进行虚拟ServerID迁移，
                 * 若是旧的回归，因为是第一个任务变更，那只能说明旧服务的数据已经迁移完成，
                 * 此时也只是需要判断是否需要进行虚拟ServerID迁移
                 */

                // ServerInfo server = getServerInfofromCache(serverId);
                List<String> virtualServerIds = identification.listVirtualIdentification();
                if (virtualServerIds != null && virtualServerIds.size() > 0) {// 说明目前在使用virtual SID
                    Collections.sort(virtualServerIds);
                    String needRecoverId = virtualServerIds.get(0);
                    // 构造任务
                    BalanceTaskSummary taskSummary = genVirtualTask(needRecoverId, changeSummary);
                    dispatchTask(taskSummary);
                    identification.invalidVirtualIden(needRecoverId);

                }

            } else if (changeSummary.getChangeType() == ChangeType.REMOVE) {
                /*
                 * 根据当时的的情况来判定，决策者如何决定，分为三种
                 * 1.该SN正常，未做任何操作
                 * 2.该SN正在进行virtual serverID恢复，此时分为两种，1.移除的机器为正在进行virtual ID映射的机器，2.移除的机器为其他参与者的机器
                 * 3.该SN正在进行副本丢失迁移，此时会根据副本数来决定迁移是否继续。
                 */
                for (int i = 1; i < changeSummarys.size(); i++) {
                    ChangeSummary tmp = changeSummarys.get(i);
                    String tempServerId = tmp.getChangeServer();
                    if (StringUtils.equals(serverId, tempServerId)) {
                        if (tmp.getChangeType() == ChangeType.ADD) {
                            BalanceTaskSummary taskSummary = genBalanceTask(changeSummary);
                            if (monitor.getTaskProgress() < 0.6) { // TODO 0.6暂时填充
                                cancelTask(taskSummary);
                                cleanChangeSummary(changeSummary, tmp);
                            }
                        }
                    }

                }
                BalanceTaskSummary taskSummary = genBalanceTask(changeSummary);
                dispatchTask(taskSummary);
            }
        }

    }

    public ServerInfo getServerInfofromCache(String serverId) {
        return new ServerInfo();
    }

    public TaskDispatch(String zkUrl, String basePath, ZookeeperIdentification identification) {
        this.zkUrl = zkUrl;
        this.basePath = basePath;
        this.identification = identification;
    }

    public void start() throws Exception {
        client = CuratorClient.getClientInstance(Preconditions.checkNotNull(zkUrl, "zkUrk is not null!"));
        client.getInnerClient().getConnectionStateListenable().addListener(new ConnectionStateListener() {
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
        client.blockUntilConnected();

        String trimBasePath = BrStringUtils.trimBasePath(Preconditions.checkNotNull(basePath, "basePath is not null!"));
        String leaderPath = trimBasePath + Constants.SEPARATOR + Constants.LEADER_NODE;
        if (!client.checkExists(leaderPath)) {
            client.createPersistent(leaderPath, true);
        }
        System.out.println("leader path:" + leaderPath);
        leaderLath = new LeaderLatch(client.getInnerClient(), leaderPath);
        leaderLath.start();

        String monitorPath = trimBasePath + Constants.SEPARATOR + Constants.CHANGE_NODE;
        System.out.println("monitorPath:" + monitorPath);
        treeCache = CuratorTreeCache.getTreeCacheInstance(zkUrl);
        treeCache.addListener(monitorPath, new TaskDispatchListener("task_dispatch"));
        treeCache.startPathCache(monitorPath);
    }

    private BalanceTaskSummary genVirtualTask(String virtualId, ChangeSummary changeSummary) {
        BalanceTaskSummary taskSummary = new BalanceTaskSummary();
        taskSummary.setOutputServers(changeSummary.getCurrentServers());
        taskSummary.setServerId(virtualId);
        taskSummary.setInputServers(Lists.asList(changeSummary.getChangeServer(), null));
        taskSummary.setTaskStatus(1);
        taskSummary.setStorageIndex(changeSummary.getStorageIndex());
        taskSummary.setRuntime(System.currentTimeMillis() / 1000 + 60 * 1); // 1分钟后开始迁移
        return taskSummary;
    }

    private BalanceTaskSummary genBalanceTask(ChangeSummary changeSummary) {
        return new BalanceTaskSummary();
    }

    public boolean dispatchTask(BalanceTaskSummary taskSummary) {
        return false;
    }

    public void cancelTask(BalanceTaskSummary taskSummary) {

    }

    public void cleanChangeSummary(ChangeSummary c1, ChangeSummary c2) {

    }

    @Override
    public void close() throws IOException {
        if (leaderLath != null) {
            leaderLath.close();
        }

        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperIdentification identification = null;  // TODO
        TaskDispatch td = new TaskDispatch(Constants.zkUrl, Constants.BASE_PATH, identification);
        td.start();
        Thread.sleep(Long.MAX_VALUE);
        td.close();

    }

}
