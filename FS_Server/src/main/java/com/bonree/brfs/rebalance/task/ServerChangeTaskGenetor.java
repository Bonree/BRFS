package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.bonree.mail.worker.MailWorker;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private LeaderLatch leaderLath;

    private String leaderPath;

    private String changesPath;

    private ServerIDManager idManager;

    private ServiceManager serverManager;

    private CuratorClient client;

    private StorageRegionManager snManager;

    private int delayDeal;

    public ServerChangeTaskGenetor(final CuratorFramework client, final ServiceManager serverManager, ServerIDManager idManager,
                                   final String baseRebalancePath, final int delayDeal, StorageRegionManager snManager)
        throws Exception {
        this.serverManager = serverManager;
        this.snManager = snManager;
        this.delayDeal = delayDeal;
        this.leaderPath = ZKPaths.makePath(baseRebalancePath, Constants.CHANGE_LEADER);
        this.changesPath = ZKPaths.makePath(baseRebalancePath, Constants.CHANGES_NODE);
        this.client = CuratorClient.wrapClient(client);
        this.leaderLath = new LeaderLatch(client, this.leaderPath);
        leaderLath.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                LOG.info("I'am not ServerChangeTaskGenetor leader!");
            }

            @Override
            public void isLeader() {
                LOG.info("I'am ServerChangeTaskGenetor leader!");
            }
        });
        this.idManager = idManager;
        leaderLath.start();
    }

    private void genChangeSummary(Service service, ChangeType type) {
        String firstID = service.getServiceId();
        List<StorageRegion> snList = snManager.getStorageRegionList();
        List<String> currentServers = getCurrentServers(serverManager);
        LOG.info("fetch all storageRegion:" + snList);
        for (StorageRegion snModel : snList) {
            if (snModel.getReplicateNum() > 1) { // TODO 此处需要判断是否配置了sn恢复
                String secondID = idManager.getOtherSecondID(firstID, snModel.getId());
                if (!StringUtils.isEmpty(secondID)) {
                    try {
                        ChangeSummary tsm = new ChangeSummary(snModel.getId(), genChangeID(), type, secondID, currentServers);
                        String jsonStr = JsonUtils.toJsonString(tsm);
                        String snTaskNode = ZKPaths.makePath(changesPath, String.valueOf(snModel.getId()), tsm.getChangeID());
                        client.createPersistent(snTaskNode, true, jsonStr.getBytes(StandardCharsets.UTF_8));
                        LOG.info("generator a change record:" + jsonStr + ", for storageRegion:" + snModel);
                        if (ChangeType.REMOVE == type) {
                            EmailPool emailPool = EmailPool.getInstance();
                            emailPool.sendEmail(MailWorker.newBuilder(emailPool.getProgramInfo()).setMessage(jsonStr));
                        }
                    } catch (Exception e) {
                        LOG.error("generator a change record failed for storageRegion:" + snModel, e);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 概述：changeID 使用时间戳和UUID进行标识
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private String genChangeID() {
        return (Calendar.getInstance().getTimeInMillis() / 1000) + UUID.randomUUID().toString();
    }

    /**
     * 概述：获取当时存活的机器
     *
     * @param serviceManager
     *
     * @return
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private List<String> getCurrentServers(ServiceManager serviceManager) {
        List<Service> servers = serviceManager
            .getServiceListByGroup(Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
        List<String> serverIDs = servers.stream().map(Service::getServiceId).collect(Collectors.toList());
        return serverIDs;
    }

    /**
     * 概述：只需要添加每次变更的信息即可
     *
     * @param zkUrl
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceAdded(Service service) {
        LOG.info("trigger a add change, service:" + service);
        try {
            Thread.sleep(delayDeal);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (leaderLath.hasLeadership()) {
            genChangeSummary(service, ChangeType.ADD);
        }

    }

    /**
     * 概述：只需要添加每次变更的信息即可
     *
     * @param zkUrl
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Override
    public void serviceRemoved(Service service) {
        LOG.info("trigger a remove change, service:" + service);
        try {
            Thread.sleep(delayDeal);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (leaderLath.hasLeadership()) {
            genChangeSummary(service, ChangeType.REMOVE);
        }

    }
}
