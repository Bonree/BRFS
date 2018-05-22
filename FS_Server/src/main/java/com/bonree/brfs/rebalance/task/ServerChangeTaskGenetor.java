package com.bonree.brfs.rebalance.task;

import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.server.identification.ServerIDManager;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月17日 上午11:16:51
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 为每个SN记录server改变的情况，以时间戳生成记录
 ******************************************************************************/
public class ServerChangeTaskGenetor implements ServiceStateListener {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChangeTaskGenetor.class);

    private static final String CHANGES_NODE = "changes";

    private LeaderLatch leaderLath;

    private String leaderPath;

    private String changesPath;

    private ServerIDManager idManager;

    private ServiceManager serverManager;

    private CuratorClient client;

    private StorageNameManager snManager;

    private CuratorClient leaderClient;

    private int delayDeal;

    public ServerChangeTaskGenetor(final CuratorClient leaderClient, final CuratorClient client, final ServiceManager serverManager, ServerIDManager idManager, final String baseRebalancePath, final int delayDeal, StorageNameManager snManager) throws Exception {
        this.serverManager = serverManager;
        this.snManager = snManager;
        this.delayDeal = delayDeal;
        this.leaderPath = baseRebalancePath + Constants.SEPARATOR + Constants.CHANGE_LEADER;
        this.changesPath = baseRebalancePath + Constants.SEPARATOR + CHANGES_NODE;
        this.client = client;
        this.leaderClient = leaderClient;
        this.leaderLath = new LeaderLatch(this.leaderClient.getInnerClient(), this.leaderPath);
        leaderLath.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {

            }

            @Override
            public void isLeader() {
                LOG.info("I'am ServerChangeTaskGenetor leader!!!!");
            }
        });
        this.idManager = idManager;
        leaderLath.start();
        LOG.info("ServerChangeTaskGenetor launch successful!!");
    }

    private void genChangeSummary(Service service, ChangeType type) {
        String firstID = service.getServiceId();
        List<StorageNameNode> snList = getStorageCache(snManager);
        for (StorageNameNode snModel : snList) {
            if (snModel.getReplicateCount() > 1 && true) { // TODO 此处需要判断是否配置了sn恢复
                List<String> currentServers = getCurrentServers(serverManager);
                String secondID = idManager.getOtherSecondID(firstID, snModel.getId());
                if (!StringUtils.isEmpty(secondID)) { // 如果没数据，该sn的secondID会为null
                    ChangeSummary tsm = new ChangeSummary(snModel.getId(), genChangeID(), type, secondID, currentServers);
                    String snPath = changesPath + Constants.SEPARATOR + snModel.getId();
                    String jsonStr = JSON.toJSONString(tsm);
                    String snTaskNode = snPath + Constants.SEPARATOR + tsm.getChangeID();
                    client.createPersistent(snTaskNode, true, jsonStr.getBytes());
                }
            }
        }
    }

    private String genChangeID() {
        return (Calendar.getInstance().getTimeInMillis() / 1000) + UUID.randomUUID().toString();
    }

    private List<StorageNameNode> getStorageCache(StorageNameManager snManager) {
        return snManager.getStorageNameNodeList();
    }

    private List<String> getCurrentServers(ServiceManager serviceManager) {
        List<Service> servers = serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
        List<String> serverIDs = servers.stream().map(Service::getServiceId).collect(Collectors.toList());
        return serverIDs;
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceAdded(Service service) {
        LOG.info("add wzlistener:"+service);
        try {
            Thread.sleep(delayDeal);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (leaderLath.hasLeadership()) {
            String firstID = service.getServiceId();
            if (firstID.equals(idManager.getFirstServerID())) {
                return;
            }
            genChangeSummary(service, ChangeType.ADD);
        }

    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceRemoved(Service service) {
        LOG.info("remove wzlistener:"+service);
        try {
            Thread.sleep(delayDeal);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (leaderLath.hasLeadership()) {
            String firstID = service.getServiceId();
            if (firstID.equals(idManager.getFirstServerID())) {
                return;
            }
            genChangeSummary(service, ChangeType.REMOVE);
        }

    }
}
