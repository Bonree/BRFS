package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.datastream.file.DuplicateNodeChecker;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.resource.vo.ResourceModel;
import com.google.common.collect.Lists;
import java.util.Collection;
import org.slf4j.Logger;

public class ResourceWriteSelector implements DuplicateNodeSelector {
    private Logger log;
    private ClusterResource daemon;
    private ServiceSelector resourceSelector;
    private String groupName;
    private DuplicateNodeSelector bakSelector;
    private DuplicateNodeChecker checker;

    public ResourceWriteSelector(ClusterResource daemon,
                                 ServiceSelector resourceSelector,
                                 DuplicateNodeSelector bakSelector,
                                 String groupName,
                                 Logger log,
                                 DuplicateNodeChecker checker) {
        this.daemon = daemon;
        this.resourceSelector = resourceSelector;
        this.groupName = groupName;
        this.bakSelector = bakSelector;
        this.log = log;
        this.checker = checker;
    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        long start = System.currentTimeMillis();
        DuplicateNode[] duplicateNodes;
        try {
            // 获取资源信息
            Collection<ResourceModel> resources = daemon.getClusterResources();
            log.info("service num in the cache: [{}], ip is [{}].", resources.size(), Lists.newArrayList(resources));
            // 采集资源未上传则使用备用选择器
            if (resources == null || resources.isEmpty()) {
                log.warn("[{}] select resource list is empty !!!! use bak selector", groupName);
                return this.bakSelector.getDuplicationNodes(storageId, nums);
            }
            // 按策略获取服务
            Collection<ResourceModel> result = this.resourceSelector.selector(resources, nums);
            if (result == null || result.isEmpty()) {
                log.error("[{}] no service can write !!!", groupName);
                return new DuplicateNode[0];
            }
            if (result.size() != nums) {
                log.warn("[{}] service can write !!!need {} but give {} ", groupName, nums, result.size());
            }
            boolean reSelect = false;
            Collection<ResourceModel> reSelectResources = daemon.getClusterResources();
            log.info("second choose service num in the cache: [{}], ip is [{}].",
                     reSelectResources.size(),
                     Lists.newArrayList(reSelectResources));
            for (ResourceModel resource : result) {
                if (checker.isChecking(new DuplicateNode(groupName, resource.getServerId(), null))) {
                    log.info("resource [{}] is being checked.", resource.getHost());
                    reSelectResources.remove(resource);
                    reSelect = true;
                }
            }
            if (reSelect) {
                result = this.resourceSelector.selector(reSelectResources, nums);
                if (result == null || result.isEmpty()) {
                    log.error("[{}] no service can write when reselect!!!", groupName);
                    return new DuplicateNode[0];
                }
                if (result.size() != nums) {
                    log.warn("[{}] service can write when reselect !!!need {} but give {} ", groupName, nums, result.size());
                }
            }
            duplicateNodes = new DuplicateNode[result.size()];
            int index = 0;
            for (ResourceModel resource : result) {
                duplicateNodes[index] = new DuplicateNode(groupName, resource.getServerId(), null);
                index++;
            }

            log.info("service num finally choosed: [{}]", duplicateNodes.length);
            return duplicateNodes;
        } finally {
            long stop = System.currentTimeMillis();
            log.info("resource select node time: {} ms", (stop - start));
        }
    }
}
