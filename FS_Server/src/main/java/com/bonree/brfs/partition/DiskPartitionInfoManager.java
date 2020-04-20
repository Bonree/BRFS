package com.bonree.brfs.partition;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractPathChildrenCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorPathCache;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.PartitionIdsConfigs;
import com.bonree.brfs.partition.model.PartitionInfo;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/25 20:59
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 负责DataNode磁盘信息管理
 ******************************************************************************/
public class DiskPartitionInfoManager implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(DiskPartitionInfoManager.class);

    private CuratorPathCache childCache;
    private ZookeeperPaths zkPath;
    private DiskPartitionInfoListener listener;
    private Map<String, PartitionInfo> diskPartitionInfoCache = new ConcurrentHashMap<>();

    @Inject
    public DiskPartitionInfoManager(ZookeeperPaths zkPath) {
        this.zkPath = zkPath;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        this.childCache = CuratorCacheFactory.getPathCache();
        this.listener = new DiskPartitionInfoListener("disk_partition_cache");
        this.childCache.addListener(ZKPaths.makePath(zkPath.getBaseDiscoveryPath(), Configs.getConfiguration().GetConfig(PartitionIdsConfigs.CONFIG_PARTITION_GROUP_NAME)), this.listener);
        LOG.info("disk partition info manager start.");
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.childCache.removeListener(ZKPaths.makePath(zkPath.getBaseDiscoveryPath(), Configs.getConfiguration().GetConfig(PartitionIdsConfigs.CONFIG_PARTITION_GROUP_NAME)), this.listener);
    }

    public PartitionInfo getPartitionInfoByPartitionId(String partitionId) {
        return diskPartitionInfoCache.get(partitionId);
    }

    public Map<String, PartitionInfo> getPartitionInfosByServiceId(String serviceId) {
        Map<String, PartitionInfo> map = new HashMap<>();
        if (serviceId == null || serviceId.isEmpty()) {
            return map;
        }

        for (Map.Entry<String, PartitionInfo> entry : diskPartitionInfoCache.entrySet()) {
            if (serviceId.equals(entry.getValue().getServiceId())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public PartitionInfo freeSizeSelector() {
        if (diskPartitionInfoCache.isEmpty()) {
            return null;
        }

        TreeMap<PartitionInfo, String> treeMap = new TreeMap<>(new DiskPartitionFreeSizeComparator());
        for (Map.Entry<String, PartitionInfo> entry : diskPartitionInfoCache.entrySet()) {
            treeMap.put(entry.getValue(), entry.getKey());
        }

        return treeMap.firstKey();

    }

    public List<String> getCurrentPartitionIds() {
        return new ArrayList<>(diskPartitionInfoCache.keySet());
    }

    private static class DiskPartitionFreeSizeComparator implements Comparator<PartitionInfo> {
        @Override
        public int compare(PartitionInfo o1, PartitionInfo o2) {
            return Double.compare(o2.getFreeSize(), o1.getFreeSize());
        }
    }

    private class DiskPartitionInfoListener extends AbstractPathChildrenCacheListener {
        private DiskPartitionInfoListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                    PartitionInfo info = JsonUtils.toObject(event.getData().getData(), PartitionInfo.class);
                    if (info != null) {
                        diskPartitionInfoCache.put(info.getPartitionId(), info);
                        LOG.info("disk partition info cache added, path:{}, info: {}", event.getData().getPath(), info);
                        LOG.info("current disk partition ids: {}", diskPartitionInfoCache.keySet());
                    }
                }
            } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                if (event.getData() != null) {
                    String partitionId = parsePartitionIdFromPath(event.getData().getPath());
                    if (diskPartitionInfoCache.containsKey(partitionId)) {
                        diskPartitionInfoCache.remove(partitionId);
                        LOG.info("disk partition info cache removed, path:{}", event.getData().getPath());
                        LOG.info("current disk partition ids: {}", diskPartitionInfoCache.keySet());
                    }
                }
            }
        }

        private String parsePartitionIdFromPath(String path) {
            return StringUtils.substringAfterLast(path, "/");
        }
    }
}
