package com.bonree.brfs.duplication.storageregion;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.CompatibilityModelConfig;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.google.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRegionBackgroundWorker implements StorageRegionStateListener {
    private static final Logger LOG = LoggerFactory.getLogger(StorageRegionStateListener.class);
    private CuratorFramework client;
    private ZookeeperPaths zkPaths;
    private SecondMaintainerInterface idManager;
    private Service service;
    private boolean compatibleV1 = false;

    public StorageRegionBackgroundWorker(CuratorFramework client, ZookeeperPaths zkPaths,
                                         SecondMaintainerInterface idManager, Service service, boolean compatibleV1) {
        this.client = client;
        this.zkPaths = zkPaths;
        this.idManager = idManager;
        this.service = service;
        this.compatibleV1 = compatibleV1;
    }

    @Inject
    public StorageRegionBackgroundWorker(
        CuratorFramework client,
        ZookeeperPaths zkPaths,
        SecondMaintainerInterface idManager,
        Service service,
        CompatibilityModelConfig config) {
        this(client, zkPaths, idManager, service, config.isCompatibilitySwitch());
    }

    @Override
    public void storageRegionAdded(StorageRegion node) {
        LOG.info("-----------StorageNameAdded--[{}]", node);
        Collection<String> second = idManager.registerSecondIds(service.getServiceId(), node.getId());
        if (compatibleV1) {
            registerV1SecondTree(node, second);
        }
    }

    /**
     * 兼容1期brfs
     *
     * @param region
     * @param seconds
     */
    public void registerV1SecondTree(StorageRegion region, Collection<String> seconds) {
        try {
            if (seconds == null || seconds.isEmpty()) {
                LOG.warn("compatible v1 model is not work,because second id set is empty");
                return;
            }
            if (seconds.size() > 1) {
                LOG.warn("compatible v1 model is invalid in mulitid partitions");
                return;
            }
            String validSecond = seconds.stream().findFirst().get();
            if (StringUtils.isEmpty(validSecond)) {
                LOG.warn("compatible v1 model is not work,because second id is empty {}", validSecond);
                return;
            }
            byte[] data = validSecond.getBytes(StandardCharsets.UTF_8);
            if (data == null || data.length == 0) {
                LOG.warn("compatible v1 model is not work,because data is empty {}", validSecond);
                return;
            }
            String zkPath = ZKPaths.makePath(zkPaths.getBaseServerIdPath(), service.getServiceId(), region.getId() + "");
            if (client.checkExists().forPath(zkPath) == null) {
                client.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.PERSISTENT)
                      .forPath(zkPath, data);
            } else {
                client.setData().forPath(zkPath, data);
            }
        } catch (Exception e) {
            LOG.error("compatible v1 model happen error ! storage {} seoncds {}", region, seconds, e);
        }
    }

    /**
     * 兼容1期brfs
     *
     * @param region
     */
    public void unRegisterV1SecondTree(StorageRegion region) {
        try {
            String zkPath = ZKPaths.makePath(zkPaths.getBaseServerIdPath(), service.getServiceId(), region.getId() + "");
            if (client.checkExists().forPath(zkPath) != null) {
                client.delete().forPath(zkPath);
            }
        } catch (Exception e) {
            LOG.error("unregister compatible v1 model happen error ! storage {} ", region, e);
        }
    }

    /**
     * 删除变更，
     *
     * @param region
     */
    public void clearWork(StorageRegion region) {
        try {
            String rebalanceBasePath = zkPaths.getBaseRebalancePath();
            String id = String.valueOf(region.getId());
            String changesPath = ZKPaths.makePath(rebalanceBasePath, Constants.CHANGES_NODE, id);
            String changesHistoryPath = ZKPaths.makePath(rebalanceBasePath, Constants.CHANGES_HISTORY_NODE, id);
            String taskPath = ZKPaths.makePath(rebalanceBasePath, Constants.TASKS_NODE, id);
            String taskHistoryPath = ZKPaths.makePath(rebalanceBasePath, Constants.TASKS_HISTORY_NODE, id);
            String routeBasepath = zkPaths.getBaseV2RoutePath();
            String virtualroutePath = ZKPaths.makePath(routeBasepath, Constants.VIRTUAL_ROUTE, id);
            String normalroutePath = ZKPaths.makePath(routeBasepath, Constants.NORMAL_ROUTE, id);

            String virtualBasePath = zkPaths.getBaseServerIdSeqPath();
            String virtualContainPath = ZKPaths.makePath(virtualBasePath, VirtualServerIDImpl.VIRTUAL_ID_CONTAINER, id);
            deleteWithChild(changesPath);
            deleteWithChild(taskPath);
            deleteWithChild(changesHistoryPath);
            deleteWithChild(taskHistoryPath);
            deleteWithChild(virtualroutePath);
            deleteWithChild(normalroutePath);
            deleteWithChild(virtualContainPath);
        } catch (Exception e) {
            LOG.error("unregister compatible v1 model happen error ! storage {} ", region, e);
        }
    }

    private void deleteWithChild(String zkPath) throws Exception {
        if (client.checkExists().forPath(zkPath) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(zkPath);
        }
    }

    @Override
    public void storageRegionUpdated(StorageRegion region) {

    }

    @Override
    public void storageRegionRemoved(StorageRegion node) {
        LOG.info("-----------StorageNameRemove--[{}]", node);
        idManager.unregisterSecondIds(service.getServiceId(), node.getId());
        if (compatibleV1) {
            unRegisterV1SecondTree(node);
        }
        clearWork(node);
    }
}
