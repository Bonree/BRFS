package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月01日 14:35:45
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 注册二级serverid维护类 datanode服务独享，client 需要使用
 ******************************************************************************/
@ManageLifecycle
public class SimpleSecondMaintainer implements SecondMaintainerInterface, LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSecondMaintainer.class);
    private LevelServerIDGen secondIdWorker;
    private CuratorFramework client = null;
    private String secondBasePath;
    private String routeBasePath;
    private SecondIdsInterface secondIds;
    private Queue<RegisterInfo> queue = new ConcurrentLinkedQueue<>();
    private ScheduledExecutorService pool = null;
    private Future<?> future = null;
    private Service localService;

    public SimpleSecondMaintainer(CuratorFramework client, Service localService, String secondBasePath, String routeBasePath,
                                  String secondIdSeqPath) {
        this.client = client;
        this.secondBasePath = secondBasePath;
        this.routeBasePath = routeBasePath;
        this.secondIdWorker = new SecondServerIDGenImpl(this.client, secondIdSeqPath);
        this.secondIds = new RetryNTimesSecondIDShip(client, secondBasePath, 3, 100);
        this.localService = localService;
    }

    @Inject
    public SimpleSecondMaintainer(CuratorFramework client, ZookeeperPaths path, Service localService) {
        this(client, localService, path.getBaseV2SecondIDPath(), path.getBaseV2RoutePath(), path.getBaseServerIdSeqPath());
    }

    /**
     * 注册二级serverid，
     *
     * @param firstServer
     * @param partitionId
     * @param storageId
     *
     * @return
     */
    @Override
    public String registerSecondId(String firstServer, String partitionId, int storageId) {
        if (StringUtils.isEmpty(firstServer)) {
            throw new RuntimeException(
                "first server id is empty !! firserverId:"
                    + firstServer + ",partitionId:" + partitionId + ",storageId:" + storageId);
        }
        String secondId = this.secondIds.getSecondId(partitionId, storageId);
        if (StringUtils.isEmpty(secondId)) {
            secondId = createSecondId(partitionId, firstServer, storageId);
        }
        return secondId;
    }

    @Override
    public Collection<String> registerSecondIds(String firstServer, int storageId) {
        List<String> partitionIds = null;
        try {
            partitionIds = getValidPartitions(firstServer);
        } catch (Exception e) {
            LOG.error("storage[{}] load firstServer[{}] partitionIds happen error ", storageId, firstServer, e);
        }
        if (partitionIds == null || partitionIds.isEmpty()) {
            LOG.info("partitionId is empty for storageRegion : {}[{}]", storageId, firstServer);
            queue.add(new RegisterInfo(firstServer, storageId));
            return null;
        }
        return registerSecondIdBatch(partitionIds, firstServer, storageId);
    }

    public Collection<String> registerSecondIdBatch(Collection<String> partitionIds, String firstServer, int storageId) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return null;
        }
        List<String> secondIds = new ArrayList<>();
        for (String partitionId : partitionIds) {
            String secondId = registerSecondId(firstServer, partitionId, storageId);
            secondIds.add(secondId);
        }
        return secondIds;
    }

    private String createSecondId(String partitionId, String content, int storageId) {
        String secondId = secondIdWorker.genLevelID();
        addPartitionRelation(content, partitionId);
        String path = ZKPaths.makePath(secondBasePath, partitionId, storageId + "");
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path, secondId.getBytes(StandardCharsets.UTF_8));
            } else {
                client.setData().forPath(path, secondId.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception ignore) {
            // ignore
        }
        try {
            byte[] sdata = client.getData().forPath(path);
            if (sdata == null || sdata.length == 0) {
                throw new RuntimeException("register secondid fail ! second id is empty !!");
            }
            String zsecond = new String(sdata, StandardCharsets.UTF_8);
            if (!secondId.equals(zsecond)) {
                throw new RuntimeException("register secondid fail ! apply secondid:[" + secondId + "], zkSecondId:[" + zsecond
                                               + "]content not same  !!");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "partitionId:[" + partitionId + "],storageId:[" + storageId + "] register secondid happen error !", e);
        }
        return secondId;
    }

    @Override
    public boolean unregisterSecondId(String partitionId, int storageId) {
        String serverId = this.secondIds.getSecondId(partitionId, storageId);
        if (serverId == null) {
            return true;
        }
        try {
            String path = ZKPaths.makePath(secondBasePath, partitionId, String.valueOf(storageId));
            client.delete().quietly().forPath(path);
            return true;
        } catch (Exception e) {
            LOG.error("can not delete second server id node", e);
        }
        return false;
    }

    @Override
    public boolean unregisterSecondIds(String firstServer, int storageId) {
        List<String> partitionIds = null;
        try {
            partitionIds = getValidPartitions(firstServer);
        } catch (Exception e) {
            LOG.error("storage[{}] load firstServer[{}] partitionIds happen error ", storageId, firstServer, e);
        }
        if (partitionIds == null || partitionIds.isEmpty()) {
            return true;
        }
        return unRegisterSecondIdBatch(partitionIds, storageId);
    }

    public boolean unRegisterSecondIdBatch(Collection<String> localPartitionIds, int storageId) {
        if (localPartitionIds == null || localPartitionIds.isEmpty()) {
            return true;
        }
        boolean status = true;
        for (String partitionId : localPartitionIds) {
            status &= unregisterSecondIds(partitionId, storageId);
        }
        return status;
    }

    @Override
    public boolean isValidSecondId(String secondId, int storageId) {
        try {
            String normalPath = ZKPaths.makePath(routeBasePath, Constants.NORMAL_ROUTE);
            String siPath = ZKPaths.makePath(normalPath, storageId + "");
            if (client.checkExists().forPath(normalPath) != null && client.checkExists().forPath(siPath) != null) {
                List<String> routeNodes = client.getChildren().forPath(siPath);
                for (String routeNode : routeNodes) {
                    String routePath = ZKPaths.makePath(siPath, routeNode);
                    LOG.info("load reoute routePath {}", routePath);
                    byte[] data = client.getData().forPath(routePath);
                    NormalRouteInterface normalRoute = SingleRouteFactory.createRoute(data);
                    if (normalRoute.getBaseSecondId().equals(secondId)) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("check secondId[{}:{}] happen error {}", secondId, storageId, e);
        }
        return true;
    }

    @Override
    public void addPartitionRelation(String firstServer, String partitionId) {
        String path = ZKPaths.makePath(secondBasePath, partitionId);
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path, firstServer.getBytes(StandardCharsets.UTF_8));
            } else {
                LOG.info("partition ship is exists partition id {}({})", partitionId, firstServer);
            }
        } catch (Exception ignore) {
            LOG.info("add partition relation happen error !ignore it ! message :{} ", ignore.getMessage());

        }
        try {
            byte[] fdata = client.getData().forPath(path);
            if (fdata == null || fdata.length == 0) {
                throw new RuntimeException("add partition relationShip fail! first server id is empty !!");
            }
            String zfirst = new String(fdata, StandardCharsets.UTF_8);
            if (!firstServer.equals(zfirst)) {
                throw new RuntimeException(
                    "add partition relationShip fail ! firstServerId:[" + firstServer + "], zk first:[" + zfirst
                        + "]content not same  !!");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "add partition relationShip fail !! [" + partitionId + "],firstserver:[" + firstServer + "] ", e);

        }
    }

    @Override
    public void addAllPartitionRelation(Collection<String> partitionIds, String firstServer) {
        if (partitionIds == null || partitionIds.isEmpty() || StringUtils.isEmpty(firstServer)) {
            throw new RuntimeException("first server [ " + firstServer + "] no partitionid to used ");
        }
        for (String partitionId : partitionIds) {
            addPartitionRelation(firstServer, partitionId);
        }
    }

    @Override
    public boolean removePartitionRelation(String partitionid) {
        try {
            String path = ZKPaths.makePath(this.secondBasePath, partitionid);
            client.delete().quietly().forPath(path);
            return true;
        } catch (Exception e) {
            LOG.error("can not delete second server id node", e);
        }
        return false;
    }

    @Override
    public boolean removeAllPartitionRelation(Collection<String> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return true;
        }
        boolean status = true;
        for (String partitionId : partitionIds) {
            status &= removePartitionRelation(partitionId);
        }
        return status;
    }

    private List<String> getValidPartitions(String firstServer) throws Exception {
        List<String> partitions = client.getChildren().forPath(secondBasePath);
        List<String> validPartitions = new ArrayList<>();
        for (String partition : partitions) {
            String path = ZKPaths.makePath(secondBasePath, partition);
            byte[] data = client.getData().forPath(path);
            // 无效的节点跳过
            if (data == null || data.length == 0) {
                LOG.warn("secondId find invalid node [{}]", path);
                continue;
            }
            String tmpF = new String(data, StandardCharsets.UTF_8);
            if (firstServer.equals(tmpF)) {
                validPartitions.add(partition);
            }
        }
        return validPartitions;
    }

    @Override
    public Collection<String> getSecondIds(String serverId, int storageRegionId) {
        return this.secondIds.getSecondIds(serverId, storageRegionId);
    }

    @Override
    public String getSecondId(String partitionId, int storageRegionId) {
        return this.secondIds.getSecondId(partitionId, storageRegionId);
    }

    @Override
    public String getFirstId(String secondId, int storageRegionId) {
        return this.secondIds.getFirstId(secondId, storageRegionId);
    }

    @Override
    public String getPartitionId(String secondId, int storageRegionId) {
        return this.secondIds.getPartitionId(secondId, storageRegionId);
    }

    @Override
    public Map<String, String> getSecondFirstRelationship(int storageRegionId) {
        return this.secondIds.getSecondFirstRelationship(storageRegionId);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        LOG.info("second maintainer thread start !!");
        // 1.检查服务的二级server是否失效
        checkSecondIds(localService);
        // 2.注册后台修复线程
        pool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("SecondIdMaintainer").build());
        future = pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                RegisterInfo info = null;
                int count = 0;
                while (queue != null && !queue.isEmpty()) {
                    info = queue.poll();
                    if (info == null) {
                        continue;
                    }
                    Collection<String> partitionIds = getSecondIds(info.getFirstId(), info.getStorageId());
                    if (partitionIds == null || partitionIds.isEmpty()) {
                        LOG.warn("{} server no partition to register secondId");
                        continue;
                    }
                    registerSecondIds(info.getFirstId(), info.getStorageId());
                    count += 1;
                }
                if (count > 0) {
                    LOG.info("second id maintain thread fix {} seconIds!", count);
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void checkSecondIds(Service local) {
        String firstServerId = local.getServiceId();
        Collection<String> partitions = null;
        try {
            partitions = getValidPartitions(firstServerId);
        } catch (Exception e) {
            throw new RuntimeException(
                MessageFormat.format("service:[{0}]check secondId happen error. when get partitions", local), e);
        }
        if (partitions == null && partitions.isEmpty()) {
            throw new RuntimeException(MessageFormat.format("first server id [{0}] no partitionId", firstServerId));
        }
        partitions.stream().forEach(
            partition -> {
                try {
                    String partitionPath = ZKPaths.makePath(this.secondBasePath, partition);
                    if (this.client.checkExists().forPath(partitionPath) == null) {
                        return;
                    }
                    byte[] fistData = this.client.getData().forPath(partitionPath);
                    if (fistData == null || fistData.length == 0) {
                        addPartitionRelation(firstServerId, partition);
                    }
                    String checkFirst = new String(fistData, "utf-8");
                    if (!checkFirst.equals(firstServerId)) {
                        throw new RuntimeException(
                            MessageFormat
                                .format("find unexpect first id [{0}]! expect:[{1}] partitionId:[{2}]", checkFirst, firstServerId,
                                        partition));
                    }
                    List<String> childs = this.client.getChildren().forPath(partitionPath);
                    if (childs == null || childs.isEmpty()) {
                        return;
                    }
                    Collections.sort(childs);
                    childs.stream().forEach(region -> {
                        int storageIndex = Integer.parseInt(region);
                        String secondId = getSecondId(region, storageIndex);
                        boolean valid = isValidSecondId(secondId, storageIndex);
                        if (!valid) {
                            String newSecondId = createSecondId(partition, firstServerId, storageIndex);
                            LOG.info("re-register secondId storageId[{}],partitionId[{}] old:[{}] new:[{}]", storageIndex,
                                     partition, secondId, newSecondId);
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException("check secondId happen error", e);
                }
            }
        );
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (future != null && !future.isCancelled()) {
            future.cancel(true);
        }
        if (pool != null) {
            pool.shutdown();
        }
    }

    private class RegisterInfo {
        private String firstId;
        private int storageId;

        public RegisterInfo(String firstId, int storageId) {
            this.firstId = firstId;
            this.storageId = storageId;
        }

        public String getFirstId() {
            return firstId;
        }

        public void setFirstId(String firstId) {
            this.firstId = firstId;
        }

        public int getStorageId() {
            return storageId;
        }

        public void setStorageId(int storageId) {
            this.storageId = storageId;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RegisterInfo{");
            sb.append("firstId='").append(firstId).append('\'');
            sb.append(", storageId=").append(storageId);
            sb.append('}');
            return sb.toString();
        }
    }
}
