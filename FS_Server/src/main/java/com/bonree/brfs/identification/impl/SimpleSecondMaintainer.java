package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.identification.LevelServerIDGen;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.rebalance.route.factory.SingleRouteFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
    private BlockingQueue<RegisterInfo> queue = new LinkedBlockingQueue<>();
    private ExecutorService pool = null;

    public SimpleSecondMaintainer(CuratorFramework client, String secondBasePath, String routeBasePath, String secondIdSeqPath) {
        this.client = client;
        this.secondBasePath = secondBasePath;
        this.routeBasePath = routeBasePath;
        this.secondIdWorker = new SecondServerIDGenImpl(this.client, secondIdSeqPath);
        this.secondIds = new RetryNTimesSecondIDShip(client, secondBasePath, 3, 100);
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
            String siPath = ZKPaths.makePath(normalPath, secondId);
            if (client.checkExists().forPath(normalPath) != null && client.checkExists().forPath(siPath) != null) {
                List<String> routeNodes = client.getChildren().forPath(siPath);
                for (String routeNode : routeNodes) {
                    String routePath = ZKPaths.makePath(siPath, routeNode);
                    byte[] data = client.getData().forPath(routePath);
                    NormalRouteInterface normalRoute = SingleRouteFactory.createRoute(data);
                    if (normalRoute.getBaseSecondId().equals(secondId)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("check secondId[{}:{}] happen error {}", secondId, storageId, e);
        }
        return false;
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
            return;
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

    @LifecycleStart
    @Override
    public void start() throws Exception {
        LOG.info("second maintainer thread start !!");
        pool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("SecondIdMaintainer").build());
        pool.execute(new Runnable() {
            @Override
            public void run() {
                RegisterInfo info = null;
                do {
                    try {
                        info = queue.take();
                        registerSecondIds(info.getFirstId(), info.getStorageId());
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.error("repeat register secondId happen error ", e);
                    }
                } while (info.storageId < 0);
                LOG.info("second id maintain thread shutdown !!");
            }
        });
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (pool != null) {
            queue.clear();
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
    }
}
