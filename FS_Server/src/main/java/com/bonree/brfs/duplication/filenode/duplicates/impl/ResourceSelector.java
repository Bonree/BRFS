package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.duplication.datastream.file.DuplicateNodeChecker;
import com.bonree.brfs.duplication.filenode.duplicates.ClusterResource;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.ServiceSelector;
import com.bonree.brfs.identification.SecondIdsInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月07日 00:00
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class ResourceSelector extends ResourceWriteSelector {
    public static final Logger LOG = LoggerFactory.getLogger(ResourceSelector.class);
    private PartitionNodeSelector nodeSelector;
    private SecondIdsInterface secondIds;

    public ResourceSelector(ClusterResource daemon,
                            ServiceSelector resourceSelector,
                            DuplicateNodeSelector bakSelector,
                            String groupName,
                            PartitionNodeSelector nodeSelector,
                            SecondIdsInterface secondIds,
                            DuplicateNodeChecker checker) {
        super(daemon, resourceSelector, bakSelector, groupName, LOG, checker);
        this.nodeSelector = nodeSelector;
        this.secondIds = secondIds;
    }

    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
        DuplicateNode[] nodes = super.getDuplicationNodes(storageId, nums);
        if (nodes == null || nodes.length == 0) {
            return nodes;
        }
        List<DuplicateNode> duplicateNodes = new ArrayList<>();
        for (DuplicateNode node : nodes) {
            String pid = this.nodeSelector.getPartitionId(node.getId());
            if (StringUtils.isEmpty(pid)) {
                continue;
            }
            String secondId = secondIds.getSecondId(pid, storageId);
            if (StringUtils.isEmpty(secondId)) {
                continue;
            }

            duplicateNodes.add(new DuplicateNode(node.getGroup(), node.getId(), secondId));
        }
        // 升序排列，如果存在降序，则在发生多次副本迁移后，影响副本的读取
        Collections.sort(duplicateNodes);
        return duplicateNodes.isEmpty() ? new DuplicateNode[0] : duplicateNodes.toArray(new DuplicateNode[duplicateNodes.size()]);
    }
}
