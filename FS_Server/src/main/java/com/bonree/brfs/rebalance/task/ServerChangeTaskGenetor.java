package com.bonree.brfs.rebalance.task;

import java.util.ArrayList;
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
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.server.StorageName;
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

    private CuratorClient client; // TODO 可以替换成zkURL

    private CuratorClient leaderClient;

    private int delayDeal;

    public ServerChangeTaskGenetor(final CuratorClient leaderClient, final CuratorClient client, final ServiceManager serverManager, ServerIDManager idManager, final String baseRebalancePath, final int delayDeal) throws Exception {
        this.serverManager = serverManager;
        this.delayDeal = delayDeal;
        this.leaderPath = baseRebalancePath + Constants.SEPARATOR + Constants.CHANGE_LEADER;
        this.changesPath = baseRebalancePath + Constants.SEPARATOR + CHANGES_NODE;
        this.client = client;
        this.leaderClient = leaderClient;
        this.leaderLath = new LeaderLatch(this.leaderClient.getInnerClient(), this.leaderPath);
        leaderLath.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                // TODO Auto-generated method stub

            }

            @Override
            public void isLeader() {
                System.out.println("I'am ServerChangeTaskGenetor leader!!!!");
            }
        });
        this.idManager = idManager;
        leaderLath.start();
        LOG.info("ServerChangeTaskGenetor launch successful!!");
    }

    private void genChangeSummary(Service service, ChangeType type) {
        String firstID = service.getServiceId();
        List<StorageName> snList = getStorageCache(); // TODO 模拟sn缓存
        for (StorageName snModel : snList) {
            if (snModel.getReplications() > 1 && snModel.isRecover()) {
                List<String> currentServers = getCurrentServers(serverManager); // TODO 需要获取当前的机器
                String secondID = idManager.getOtherSecondID(firstID, snModel.getIndex());
                if (!StringUtils.isEmpty(secondID)) { // 如果没数据，该sn的secondID会为null
                    ChangeSummary tsm = new ChangeSummary(snModel.getIndex(), genChangeID(), type, secondID, currentServers);
                    String snPath = changesPath + Constants.SEPARATOR + snModel.getIndex();
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

    private List<StorageName> getStorageCache() {
        StorageName sn = new StorageName();
        sn.setIndex(1);
        sn.setStorageName("sdk");
        sn.setDescription("sdk");
        sn.setReplications(2);
        sn.setRecover(true);
        sn.setTtl(1000000);
        sn.setTriggerRecoverTime(5454455544l);

        StorageName sn1 = new StorageName();
        sn1.setIndex(2);
        sn1.setStorageName("v4");
        sn1.setDescription("v4");
        sn1.setReplications(2);
        sn1.setRecover(true);
        sn1.setTtl(1000000);
        sn1.setTriggerRecoverTime(5454455544l);
        List<StorageName> tmp = new ArrayList<StorageName>();
        tmp.add(sn);
        // tmp.add(sn1);
        return tmp;
    }

    private List<String> getCurrentServers(ServiceManager serviceManager) {
        List<Service> servers = serviceManager.getServiceListByGroup(Constants.DISCOVER);
        List<String> serverIDs = servers.stream().map(Service::getServiceId).collect(Collectors.toList());
        return serverIDs;
    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceAdded(Service service) {
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
            System.out.println("add:" + service);
            genChangeSummary(service, ChangeType.ADD);
        }

    }

    /** 概述：只需要添加每次变更的信息即可
     * @param zkUrl
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceRemoved(Service service) {
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
            System.out.println("remove:" + service);
            genChangeSummary(service, ChangeType.REMOVE);
        }

    }
}
