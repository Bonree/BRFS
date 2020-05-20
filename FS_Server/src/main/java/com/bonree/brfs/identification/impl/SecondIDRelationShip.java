package com.bonree.brfs.identification.impl;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2020年4月11日 下午3:31:55
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 磁盘节点对应的二级server关系，只查询zk上的id关系，不对其做任何修改。
 ******************************************************************************/
public class SecondIDRelationShip implements SecondIdsInterface {
    private static final Logger LOG = LoggerFactory.getLogger(SecondIDRelationShip.class);
    private static final String SEPARATOR = ":";
    /**
     * 二级serverid的基本路径
     */
    private String secondIdBasPath = null;
    /**
     * 一级serverid与磁盘id的对应关系
     */
    private Map<String, Collection<String>> firstToPartitionIdMap = new ConcurrentHashMap<>();
    /**
     * 磁盘id与一级serverId的对应关系
     */
    private Map<String, String> partitionTofirstMap = new ConcurrentHashMap<>();

    /**
     * storageRegionId +partitionId 与二级serverId的关系
     * key为 storageid+partitionid
     * value为 secondId
     */
    private Map<String, String> secondIDsMap = new ConcurrentHashMap<>();
    /**
     * storageRegionId + secondId 与 partitionId的关系
     */
    private Map<String, String> partitionIDSMap = new ConcurrentHashMap<>();
    /**
     * curator 目录缓存
     */
    private CuratorTreeCache secondIDCache = null;
    /**
     * curator客户端
     */
    private CuratorFramework client = null;

    private SecondIDCacheListerner listerner;

    public SecondIDRelationShip(CuratorFramework client, String secondIdBasPath) throws Exception {
        this.secondIdBasPath = secondIdBasPath;
        this.client = client;
        load();
        secondIDCache = CuratorCacheFactory.getTreeCache();
        listerner = new SecondIDCacheListerner(this.secondIdBasPath);
        secondIDCache.addListener(this.secondIdBasPath, listerner);
    }

    @Override
    public Collection<String> getSecondIds(String serverId, int storageRegionId) {
        List<String> secondIds = new ArrayList<>();
        while (true) {
            try {
                Collection<String> partitionIds = firstToPartitionIdMap.get(serverId);
                if (partitionIds != null && !partitionIds.isEmpty()) {
                    for (String partitionId : partitionIds) {
                        String second = getSecondId(partitionId, storageRegionId);
                        secondIds.add(second);
                    }
                    break;
                }
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return secondIds;

    }

    @Override
    public String getSecondId(String partitionId, int storageRegionId) {
        String key = storageRegionId + SEPARATOR + partitionId;
        while (true) {
            try {
                String secondId = secondIDsMap.get(key);
                if (StringUtils.isEmpty(secondId) || StringUtils.isBlank(secondId)) {
                    Thread.sleep(50);
                } else {
                    return secondId;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String getFirstId(String secondId, int storageRegionId) {
        String key = storageRegionId + SEPARATOR + secondId;
        while (true) {
            try {
                String partitionId = this.partitionIDSMap.get(key);
                if (StringUtils.isNotEmpty(partitionId) && StringUtils.isNotBlank(partitionId)) {
                    String firstServer = this.partitionTofirstMap.get(partitionId);
                    if (StringUtils.isNotEmpty(firstServer) && StringUtils.isNotBlank(firstServer)) {
                        return firstServer;
                    }
                }
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String getPartitionId(String secondId, int storageRegionId) {
        String key = storageRegionId + SEPARATOR + secondId;
        while (true) {
            try {
                String partitionId = this.partitionIDSMap.get(key);
                if (StringUtils.isNotEmpty(partitionId) && StringUtils.isNotBlank(partitionId)) {
                    return partitionId;
                }
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Map<String, String> getSecondFirstRelationship(int storageRegionId) {
        if (partitionTofirstMap == null || partitionTofirstMap.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, String> cache = new ConcurrentHashMap<>();
        partitionTofirstMap.forEach((partitionId, firstServerId) -> {
            String second = getSecondId(partitionId, storageRegionId);
            if (second == null) {
                return;
            }
            cache.put(second, firstServerId);
        });
        return cache;
    }

    /**
     * 加载数据
     */
    private void load() throws Exception {
        if (client.checkExists().forPath(secondIdBasPath) == null) {
            return;
        }
        List<String> partitions = client.getChildren().forPath(secondIdBasPath);
        if (partitions == null || partitions.isEmpty()) {
            return;
        }
        for (String partition : partitions) {
            String ppath = this.secondIdBasPath + Constants.SEPARATOR + partition;
            byte[] data = client.getData().forPath(ppath);
            if (data == null || data.length == 0) {
                continue;
            }
            String firstServer = new String(data, StandardCharsets.UTF_8);
            packageFirstServer(firstServer, partition);
            List<String> storageRegionIds = client.getChildren().forPath(ppath);
            if (storageRegionIds != null && !storageRegionIds.isEmpty()) {
                for (String sr : storageRegionIds) {
                    String spath = ppath + Constants.SEPARATOR + sr;
                    byte[] secondData = client.getData().forPath(spath);
                    if (secondData == null || secondData.length == 0) {
                        continue;
                    }
                    String secondId = new String(secondData, StandardCharsets.UTF_8);
                    packageSecondServer(sr, partition, secondId);
                }
            }

        }
    }

    /**
     * 封装一级serverid对应关系
     *
     * @param firstServer
     * @param partition
     */
    private void packageFirstServer(String firstServer, String partition) {
        Collection<String> list = firstToPartitionIdMap.get(firstServer);
        if (list == null || list.isEmpty()) {
            firstToPartitionIdMap.put(firstServer, new CopyOnWriteArraySet<>());
            list = firstToPartitionIdMap.get(firstServer);
        }
        list.add(partition);
        partitionTofirstMap.put(partition, firstServer);
    }

    private void removeFirstServer(String firstServer, String partition) {
        Collection<String> list = firstToPartitionIdMap.get(firstServer);
        if (list != null && list.size() == 1) {
            firstToPartitionIdMap.remove(firstServer);
        } else if (list != null && list.size() > 1) {
            list.remove(firstServer);
        }
        partitionTofirstMap.remove(partition);
    }

    private void packageSecondServer(String storageRegionId, String partition, String second) {
        String key = storageRegionId + SEPARATOR + partition;
        secondIDsMap.put(key, second);
        String skey = storageRegionId + SEPARATOR + second;
        partitionIDSMap.put(skey, partition);
    }

    private void removeSecondServer(String storageRegionId, String partition, String second) {
        String key = storageRegionId + SEPARATOR + partition;
        secondIDsMap.remove(key);
        String skey = storageRegionId + SEPARATOR + second;
        partitionIDSMap.remove(skey);
    }

    /**
     * 二级serverid监听器 用于实时更新缓存
     */
    private class SecondIDCacheListerner implements TreeCacheListener {
        private final Logger log = LoggerFactory.getLogger(SecondIDCacheListerner.class);
        /**
         * 二级serverid的根目录，用来区分磁盘信息以及
         */
        private String secondIdBasPath = null;

        public SecondIDCacheListerner(String secondIdBasPath) {
            this.secondIdBasPath = secondIdBasPath;
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            ChildData childData = event.getData();
            TreeCacheEvent.Type type = event.getType();
            // 1.child数据为空则为无效事件，不进行监听
            if (childData == null) {
                log.info("Event[{}] data is empty", type);
                return;
            }
            String path = childData.getPath();
            String[] nodes = getNodeName(path);
            // 2.节点个数超过指定值，则认为无效变更，不予理会
            if (nodes == null || nodes.length == 0 || nodes.length > 2) {
                log.info("Event: [{}] seoncBasepath:[{}], path:[{}] is not secondIds", type, secondIdBasPath, path);
                return;
            }
            // 3.增加磁盘节点的处理
            if (nodes.length == 1) {
                handlerPartitionIDs(childData, type, nodes[0]);
                // 4. 增加sn节点的处理
            } else {
                handlerSecondIDs(childData, type, nodes[0], nodes[1]);
            }
        }

        /**
         * 处理partitionId事件
         *
         * @param childData
         * @param type
         * @param partitionId
         */
        private void handlerPartitionIDs(ChildData childData, TreeCacheEvent.Type type, String partitionId) {
            if (childData.getData() == null || childData.getData().length == 0) {
                return;
            }
            String firstServer = new String(childData.getData(), StandardCharsets.UTF_8);
            if (StringUtils.isEmpty(firstServer) || StringUtils.isBlank(firstServer)) {
                log.info("Invalid first server Event:[{}], partitionId:[{}]", type, partitionId);
                return;
            }
            log.info(" Event:[{}], partitionId:[{}] firstServer [{}]", type, partitionId, firstServer);
            if (TreeCacheEvent.Type.NODE_ADDED.equals(type)) {
                packageFirstServer(firstServer, partitionId);
            } else if (TreeCacheEvent.Type.NODE_REMOVED.equals(type)) {
                removeFirstServer(firstServer, partitionId);
            } else {
                log.info("Invalid Event [{}] partitionId:[{}]", type, partitionId);
            }
        }

        /**
         * 处理storage 二级serverid 事件
         *
         * @param childData
         * @param type
         * @param partitionId
         * @param storageId
         */
        private void handlerSecondIDs(ChildData childData, TreeCacheEvent.Type type, String partitionId, String storageId) {
            String key = storageId + SEPARATOR + partitionId;
            // todo 此处可能存在问题，加重测试
            if (TreeCacheEvent.Type.NODE_ADDED.equals(type) && childData.getData() != null) {
                String secondId = new String(childData.getData(), StandardCharsets.UTF_8);
                packageSecondServer(storageId, partitionId, secondId);
            } else if (TreeCacheEvent.Type.NODE_REMOVED.equals(type)) {
                String secondId = null;
                if (childData.getData() != null) {
                    secondId = new String(childData.getData(), StandardCharsets.UTF_8);
                } else {
                    secondId = getSecondId(partitionId, Integer.parseInt(storageId));
                }
                removeSecondServer(storageId, partitionId, secondId);
            } else if (TreeCacheEvent.Type.NODE_UPDATED.equals(type)) {
                String secondId = new String(childData.getData(), StandardCharsets.UTF_8);
                removeSecondServer(storageId, partitionId, secondId);
                if (StringUtils.isNotEmpty(secondId) && StringUtils.isNotBlank(secondId)) {
                    packageSecondServer(storageId, partitionId, secondId);
                }
            } else {
                // do nothing
            }
        }

        /**
         * 获取变更的节点名称
         *
         * @param path
         *
         * @return
         */
        private String[] getNodeName(String path) {
            String tmp = path.substring(path.indexOf(this.secondIdBasPath) + this.secondIdBasPath.length());
            if (StringUtils.isEmpty(tmp)) {
                return null;
            }
            String[] data = StringUtils.split(tmp, "/");
            return data;
        }
    }
}
