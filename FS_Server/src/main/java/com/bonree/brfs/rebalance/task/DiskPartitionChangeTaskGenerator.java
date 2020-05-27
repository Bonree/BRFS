package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractPathChildrenCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorPathCache;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.disknode.PartitionConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.partition.model.PartitionInfo;
import com.bonree.mail.worker.MailWorker;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/1 11:36
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 监听磁盘信息变更并发布
 ******************************************************************************/
public class DiskPartitionChangeTaskGenerator implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(DiskPartitionChangeTaskGenerator.class);

    private LeaderLatch leaderLath;
    private String leaderPath;
    private String changesPath;
    private IDSManager idManager;
    private ServiceManager serverManager;
    private CuratorClient client;
    private StorageRegionManager snManager;
    private int delayDeal;
    private ZookeeperPaths zkPath;
    private DiskPartitionInfoManager partitionInfoManager;

    private CuratorPathCache childCache;
    private DiskPartitionChangeListener listener;
    private PartitionConfig partitionConfig;

    @Inject
    public DiskPartitionChangeTaskGenerator(final CuratorFramework client, final ServiceManager serverManager,
                                            IDSManager idManager, StorageRegionManager snManager, ZookeeperPaths zkPath,
                                            DiskPartitionInfoManager partitionInfoManager, PartitionConfig partitionConfig) {
        this.serverManager = serverManager;
        this.snManager = snManager;
        this.delayDeal = 3000;
        this.zkPath = zkPath;
        this.leaderPath = ZKPaths.makePath(this.zkPath.getBaseRebalancePath(), Constants.CHANGE_LEADER);
        this.changesPath = ZKPaths.makePath(this.zkPath.getBaseRebalancePath(), Constants.CHANGES_NODE);
        this.client = CuratorClient.wrapClient(client);
        this.leaderLath = new LeaderLatch(client, this.leaderPath);
        this.idManager = idManager;
        this.partitionInfoManager = partitionInfoManager;
        this.partitionConfig = partitionConfig;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        leaderLath.addListener(new DiskPartitionChangeLeaderLatchListener());
        this.leaderLath.start();
        this.childCache = CuratorCacheFactory.getPathCache();
        this.listener = new DiskPartitionChangeListener("disk_partition_change");
        this.childCache
            .addListener(ZKPaths.makePath(zkPath.getBaseDiscoveryPath(), partitionConfig.getPartitionGroupName()), this.listener);
        LOG.info("disk partition change task generator start.");
    }

    @LifecycleStop
    @Override
    public void stop() {
        this.childCache.removeListener(
            ZKPaths.makePath(zkPath.getBaseDiscoveryPath(), partitionConfig.getPartitionGroupName()),
            this.listener);
    }

    private class DiskPartitionChangeListener extends AbstractPathChildrenCacheListener {
        public DiskPartitionChangeListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            // 该sleep的作用有2
            // 1. 防止监听事件已经发生但此时leader还未切换完成导致REMOVE事件丢失
            // 2. 目前datanode启动顺序无法人为控制，为防止二级server监听器还没有同步完数据，导致获取的二级serverid为空，造成变更创建失败
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException ignore) {
                ;
            }
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                if (leaderLath.hasLeadership()) {
                    if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                        PartitionInfo info = JsonUtils.toObject(event.getData().getData(), PartitionInfo.class);
                        if (info != null) {
                            generateChangeSummary(info, ChangeType.ADD);
                        } else {
                            LOG.warn("DiskPartitionInfo is null ! path:{},event:{}", event.getData().getPath(), event.getType());
                        }
                    }
                }
            } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                if (leaderLath.hasLeadership()) {
                    if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                        PartitionInfo info = JsonUtils.toObject(event.getData().getData(), PartitionInfo.class);
                        if (info != null) {
                            generateChangeSummary(info, ChangeType.REMOVE);
                        } else {
                            LOG.warn("DiskPartitionInfo is null ! path:{},event:{}", event.getData().getPath(), event.getType());
                        }
                    }
                }
            }
        }
    }

    private static class DiskPartitionChangeLeaderLatchListener implements LeaderLatchListener {
        @Override
        public void isLeader() {
            LOG.info("I'am DiskPartitionChangeTaskGenerator leader!");
        }

        @Override
        public void notLeader() {
            LOG.info("I'am not DiskPartitionChangeTaskGenerator leader!");
        }
    }

    private void generateChangeSummary(PartitionInfo partitionInfo, ChangeType type) {
        List<StorageRegion> snList = snManager.getStorageRegionList();
        List<String> currentServers = getCurrentServers(serverManager);

        List<String> currentPartitionIds = partitionInfoManager.getCurrentPartitionIds();

        for (StorageRegion snModel : snList) {
            if (snModel.getReplicateNum() > 1) {   // 是否配置SN恢复
                String secondID = idManager.getSecondId(partitionInfo.getPartitionId(), snModel.getId());

                if (StringUtils.isNotEmpty(secondID)) {
                    try {
                        DiskPartitionChangeSummary summaryObj =
                            new DiskPartitionChangeSummary(snModel.getId(), genChangeID(), type, secondID,
                                                           partitionInfo.getPartitionId(), currentServers, currentPartitionIds);
                        String summary = JsonUtils.toJsonString(summaryObj);
                        String diskPartitionTaskNode =
                            ZKPaths.makePath(changesPath, String.valueOf(snModel.getId()), summaryObj.getChangeID());
                        client.createPersistent(diskPartitionTaskNode, true, summary.getBytes(StandardCharsets.UTF_8));
                        LOG.info("generator a disk partition change record [{}] for storageRegion [{}]", summary, snModel);

                        if (ChangeType.REMOVE == type) {
                            EmailPool emailPool = EmailPool.getInstance();
                            emailPool.sendEmail(MailWorker.newBuilder(emailPool.getProgramInfo()).setMessage(summary));
                        }
                    } catch (Exception e) {
                        LOG.error("generator a disk partition change record failed for storageRegion: [{}]", snModel, e);
                    }
                } else {
                    LOG.warn("StorageRegion no secondID sr:[{}],partition:[{}]", snModel.getName(),
                             partitionInfo.getPartitionId());
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
        return servers.stream().map(Service::getServiceId).collect(Collectors.toList());
    }

}
