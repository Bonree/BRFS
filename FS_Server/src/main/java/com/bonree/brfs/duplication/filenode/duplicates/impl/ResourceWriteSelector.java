package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.resource.vo.ResourceModel;
import java.util.Collection;
import java.util.Iterator;
import org.slf4j.Logger;

public class ResourceWriteSelector implements DuplicateNodeSelector {
    private Logger log;
    private ClusterResource daemon;
    private ServiceSelector resourceSelector;
    private String groupName;
    private DuplicateNodeSelector bakSelector;

    public ResourceWriteSelector(ClusterResource daemon, ServiceSelector resourceSelector, DuplicateNodeSelector bakSelector,
                                 String groupName, Logger log) {
        this.daemon = daemon;
        this.resourceSelector = resourceSelector;
        this.groupName = groupName;
        this.bakSelector = bakSelector;
        this.log = log;
    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        long start = System.currentTimeMillis();
        DuplicateNode[] duplicateNodes;
        try {
            // 获取资源信息
            Collection<ResourceModel> resources = daemon.getClusterResources();
            // 采集资源未上传则使用备用选择器
            if (resources == null || resources.isEmpty()) {
                log.warn("[{}] select resource list is empty !!!! use bak selector", groupName);
                return this.bakSelector.getDuplicationNodes(storageId, nums);
            }
            // 过滤资源异常服务
            Collection<ResourceModel> selectors = this.resourceSelector.filterService(resources, null);
            if (selectors == null || selectors.isEmpty()) {
                log.error("[{}] none avaible server to selector !!!", groupName);
                return new DuplicateNode[0];
            }
            // 按策略获取服务
            Collection<ResourceModel> wins = this.resourceSelector.selector(selectors, null, nums);
            if (wins == null || wins.isEmpty()) {
                log.error("[{}] no service can write !!!", groupName);
                return new DuplicateNode[0];
            }
            if (wins.size() != nums) {
                log.warn("[{}] service can write !!!need {} but give {} ", groupName, nums, wins.size());
            }
            duplicateNodes = new DuplicateNode[wins.size()];
            Iterator<ResourceModel> iterator = wins.iterator();
            int i = 0;
            ResourceModel next;
            while (iterator.hasNext()) {
                next = iterator.next();
                duplicateNodes[i] = new DuplicateNode(groupName, next.getServerId());
                i++;
            }

            return duplicateNodes;
        } finally {
            long stop = System.currentTimeMillis();
            log.info("resource select node time: {} ms", (stop - start));
        }
    }
}
