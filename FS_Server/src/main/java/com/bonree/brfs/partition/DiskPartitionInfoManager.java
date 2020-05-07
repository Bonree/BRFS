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
import com.bonree.brfs.disknode.PartitionConfig;
import com.bonree.brfs.partition.model.PartitionInfo;
import com.bonree.brfs.resource.vo.ClusterStorageInfo;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private PathChildrenCache cache;
    private ZookeeperPaths zkPath;
    private DiskPartitionInfoListener listener;
    private Map<String, PartitionInfo> diskPartitionInfoCache = new ConcurrentHashMap<>();
    private CuratorFramework client;
    private String path;

    @Inject
    public DiskPartitionInfoManager(CuratorFramework client, ZookeeperPaths zkPath, PartitionConfig partitionIdsConfigs) {
        this.client = client;
        this.zkPath = zkPath;
        this.path = ZKPaths.makePath(zkPath.getBaseDiscoveryPath(), partitionIdsConfigs.getPartitionGroupName());
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {

        this.cache = new PathChildrenCache(client, path, true);
        this.cache.start();
        this.listener = new DiskPartitionInfoListener();
        this.cache.getListenable().addListener(this.listener);
        LOG.info("disk partition info manager start.");
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.cache.close();
        LOG.info("disk partition info manager stop. ");
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

    /**
     * @return
     */
    public ClusterStorageInfo getClusterStoragInfo() {
        if (diskPartitionInfoCache == null || diskPartitionInfoCache.isEmpty()) {
            return new ClusterStorageInfo(0, 0);
        }
        long size = diskPartitionInfoCache.values().stream().mapToLong(PartitionInfo::getTotalSize).sum();
        long remainSize = diskPartitionInfoCache.values().stream().mapToLong(PartitionInfo::getFreeSize).sum();
        return new ClusterStorageInfo(size, remainSize);
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

    private class DiskPartitionInfoListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            LOG.info("event :{} {},cacheSize:{}", event.getType(), event.getData().getPath(), diskPartitionInfoCache.size());
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
                || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
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
