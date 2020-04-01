package com.bonree.brfs.partition;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.partition.model.PartitionInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
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

    private CuratorTreeCache treeCache;
    private ZookeeperPaths zkPath;
    private Map<String, PartitionInfo> diskPartitionInfoCache = new ConcurrentHashMap<>();

    public DiskPartitionInfoManager(ZookeeperPaths zkPath) {
        this.zkPath = zkPath;
    }

    @Override
    public void start() throws Exception {
        this.treeCache = CuratorCacheFactory.getTreeCache();
        this.treeCache.addListener(ZKPaths.makePath(zkPath.getBaseClusterName(), Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME)), new DiskPartitionInfoListener());

    }

    @Override
    public void stop() throws Exception {
        this.treeCache.cancelListener(ZKPaths.makePath(zkPath.getBaseClusterName(), Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME)));
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

    private class DiskPartitionFreeSizeComparator implements Comparator<PartitionInfo> {
        @Override
        public int compare(PartitionInfo o1, PartitionInfo o2) {
            return Double.compare(o2.getFreeSize(), o1.getFreeSize());
        }
    }

    private class DiskPartitionInfoListener implements TreeCacheListener {
        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {

            if (event.getType().equals(TreeCacheEvent.Type.NODE_ADDED)) {
                if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                    PartitionInfo info = JsonUtils.toObject(event.getData().getData(), PartitionInfo.class);
                    if (info != null) {
                        diskPartitionInfoCache.put(info.getPartitionId(), info);
                        LOG.info("disk partition info cache added, path:{}, info: {}", event.getData().getPath(), info);
                    }
                }
            } else if (event.getType().equals(TreeCacheEvent.Type.NODE_REMOVED)) {
                if (event.getData() != null) {
                    String partitionId = parsePartitionIdFromPath(event.getData().getPath());
                    if (diskPartitionInfoCache.containsKey(partitionId)) {
                        diskPartitionInfoCache.remove(partitionId);
                        LOG.info("disk partition info cache removed, path:{}", event.getData().getPath());
                    }
                }
            } else if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {

                if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                    PartitionInfo info = JsonUtils.toObject(event.getData().getData(), PartitionInfo.class);

                    if (info != null) {
                        LOG.info("disk partition info cache updated, path:{}, info: [{}] -> [{}]", event.getData().getPath(), diskPartitionInfoCache.get(info.getPartitionId()), info);
                        diskPartitionInfoCache.put(info.getPartitionId(), info);
                    }
                }
            }
        }

        private String parsePartitionIdFromPath(String path) {
            return StringUtils.substringAfterLast(path, "/");
        }
    }
}
