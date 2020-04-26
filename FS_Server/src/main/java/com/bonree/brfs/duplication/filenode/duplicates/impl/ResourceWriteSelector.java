package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import java.util.Collection;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceWriteSelector implements DuplicateNodeSelector {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceWriteSelector.class);
    private ClusterResource daemon;
    private ServiceSelector resourceSelector;
    private StorageRegionManager storageRegionManager;
    private String groupName;
    private DuplicateNodeSelector bakSelector;

    public ResourceWriteSelector(ClusterResource daemon, ServiceSelector resourceSelector,
                                 StorageRegionManager storageRegionManager, DuplicateNodeSelector bakSelector, String groupName) {
        this.daemon = daemon;
        this.resourceSelector = resourceSelector;
        this.storageRegionManager = storageRegionManager;
        this.groupName = groupName;
        this.bakSelector = bakSelector;

    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        long start = System.currentTimeMillis();
        DuplicateNode[] duplicateNodes;
        try {
            StorageRegion sr = this.storageRegionManager.findStorageRegionById(storageId);
            String srName = sr.getName();
            if (sr == null) {
                LOG.error("srid : {} is not exist !!!", storageId);
                return new DuplicateNode[0];
            }
            // 获取资源信息
            Collection<ResourceModel> resources = daemon.getClusterResources();
            // 采集资源未上传则使用备用选择器
            if (resources == null || resources.isEmpty()) {
                LOG.warn("[{}] select resource list is empty !!!! use bak selector", groupName);
                return this.bakSelector.getDuplicationNodes(storageId, nums);
            }
            // 过滤资源异常服务
            Collection<ResourceModel> selectors = this.resourceSelector.filterService(resources, srName);
            if (selectors == null || selectors.isEmpty()) {
                LOG.error("[{}] none avaible server to selector !!!", groupName);
                return new DuplicateNode[0];
            }
            // 按策略获取服务
            Collection<ResourceModel> wins = this.resourceSelector.selector(selectors, srName, nums);
            if (wins == null || wins.isEmpty()) {
                LOG.error("[{}] no service can write !!!", groupName);
                return new DuplicateNode[0];
            }
            if (wins.size() != nums) {
                LOG.warn("[{}] service can write !!!need {} but give {} ", groupName, nums, wins.size());
            }
            duplicateNodes = new DuplicateNode[wins.size()];
            Iterator<ResourceModel> iterator = wins.iterator();
            int i = 0;
            StringBuilder builder = new StringBuilder();
            builder.append("select service -> ");
            ResourceModel next;
            while (iterator.hasNext()) {
                next = iterator.next();
                duplicateNodes[i] = new DuplicateNode(groupName, next.getServerId());
                i++;
                builder.append(i).append(":").append(next.getServerId()).append("(").append(next.getHost()).append(", remainSize")
                    .append(next.getLocalRemainSizeValue(srName)).append("b ), ");
            }
            LOG.info("{}", builder.toString());

            return duplicateNodes;
        } finally {
            long stop = System.currentTimeMillis();
            LOG.info("resource select node time: {} ms", (stop - start));
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private ClusterResource daemon = null;
        private ServiceSelector resourceSelector = null;
        private StorageRegionManager storageRegionManager = null;
        private String groupName = null;
        private DuplicateNodeSelector bakSelector = null;

        public Builder setGroupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder setDaemon(ClusterResource daemon) {
            this.daemon = daemon;
            return this;
        }

        public Builder setResourceSelector(ServiceSelector resourceSelector) {
            this.resourceSelector = resourceSelector;
            return this;
        }

        public Builder setStorageRegionManager(StorageRegionManager storageRegionManager) {
            this.storageRegionManager = storageRegionManager;
            return this;
        }

        public Builder setBakSelector(DuplicateNodeSelector bakSelector) {
            this.bakSelector = bakSelector;
            return this;
        }

        public ResourceWriteSelector build() {
            return new ResourceWriteSelector(this.daemon, this.resourceSelector, this.storageRegionManager, this.bakSelector,
                                             this.groupName);
        }
    }
}
