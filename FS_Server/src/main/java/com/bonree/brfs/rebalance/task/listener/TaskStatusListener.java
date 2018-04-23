package com.bonree.brfs.rebalance.task.listener;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.ChangeSummary;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskDispatcher;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月19日 下午3:48:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 监听任务是否完成，以便进行通知
 ******************************************************************************/
public class TaskStatusListener extends AbstractTreeCacheListener {
    private final static Logger LOG = LoggerFactory.getLogger(TaskStatusListener.class);

    TaskDispatcher dispatch;

    public TaskStatusListener(String listenName, TaskDispatcher dispatch) {
        super(listenName);
        this.dispatch = dispatch;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        if (dispatch.getLeaderLatch().hasLeadership()) {
            CuratorClient curatorClient = CuratorClient.wrapClient(client);
            LOG.info("leaderLath:" + dispatch.getLeaderLatch().hasLeadership());
            LOG.info("task Dispatch event detail:" + event.getType());
            System.out.println("task Dispatch event detail:" + event);
            if (event.getData() != null && event.getData().getData() != null) {
                System.out.println("content:" + new String(event.getData().getData()));
            }
            if (dispatch.isUpdatedNode(event)) {
                if (event.getData() != null && event.getData().getData() != null) {
                    // 此处会检测任务是否完成
                    String eventPath = event.getData().getPath();
                    String parentPath = StringUtils.substring(eventPath, 0, eventPath.lastIndexOf('/'));
                    BalanceTaskSummary bts = JSON.parseObject(curatorClient.getData(parentPath), BalanceTaskSummary.class);
                    List<String> serverIds = curatorClient.getChildren(parentPath);
                    System.out.println("parentPath:" + parentPath);
                    System.out.println("serverIds:" + serverIds);
                    boolean finishFlag = true;
                    if (serverIds != null && !serverIds.isEmpty()) {
                        for (String serverId : serverIds) {
                            String nodePath = parentPath + Constants.SEPARATOR + serverId;
                            TaskDetail td = JSON.parseObject(curatorClient.getData(nodePath), TaskDetail.class);
                            if (td.getStatus() != DataRecover.ExecutionStatus.FINISH) {
                                finishFlag = false;
                                break;
                            }
                            System.out.println(td + "----------" + finishFlag);
                        }
                    }
                    if (finishFlag) {// 所有的服务都则发布迁移规则，并清理任务
                        String roleNode = dispatch.getRoutePath() + Constants.SEPARATOR + bts.getStorageIndex() + Constants.SEPARATOR + Constants.ROUTE_NODE;
                        curatorClient.createPersistentSequential(roleNode, true, curatorClient.getData(parentPath));
                        // 清理变更
                        List<ChangeSummary> changeSummaries = dispatch.getSummaryCache().get(bts.getStorageIndex());

                        System.out.println("status delete:" + bts.getChangeID());
                        String changePath = dispatch.getChangesPath() + Constants.SEPARATOR + bts.getStorageIndex() + Constants.SEPARATOR + bts.getChangeID();
                        System.out.println("delete : " + changePath);
                        curatorClient.delete(changePath, false);

                        Iterator<ChangeSummary> it = changeSummaries.iterator();
                        while (it.hasNext()) {
                            if (it.next().getChangeID().equals(bts.getChangeID())) {
                                it.remove();
                            }
                        }

                        // 删除任务
                        System.out.println("delete :" + parentPath);
                        curatorClient.delete(parentPath, true);
                        if (bts.getTaskType() == RecoverType.VIRTUAL) {
                            dispatch.getServerIDManager().deleteVirtualID(bts.getStorageIndex(), bts.getServerId());
                        }
                        // 重新审计
                        dispatch.auditTask(changeSummaries);
                    }
                }
            }
        }
    }
}
