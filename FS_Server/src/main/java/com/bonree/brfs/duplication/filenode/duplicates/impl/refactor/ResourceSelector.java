package com.bonree.brfs.duplication.filenode.duplicates.impl.refactor;

import com.bonree.brfs.duplication.filenode.duplicates.*;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.SecondIdsInterface;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月07日 00:00
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class ResourceSelector extends ResourceWriteSelector {
    private PartitionNodeSelector pSelector;
    private SecondIdsInterface secondIds;

    public ResourceSelector(ClusterResource daemon, ServiceSelector resourceSelector, DuplicateNodeSelector bakSelector, String groupName, PartitionNodeSelector pSelector, SecondIdsInterface secondIds) {
        super(daemon, resourceSelector, bakSelector, groupName);
        this.pSelector = pSelector;
        this.secondIds = secondIds;
    }
    @Override
    public DuplicateNode[] getDuplicationNodes(int storageId, int nums){
        DuplicateNode[] nodes = super.getDuplicationNodes(storageId,nums);
        if(nodes == null || nodes.length == 0){
            return nodes;
        }
        List<DuplicateNode> duplicateNodes = new ArrayList<>();
        for(DuplicateNode node :nodes){
            String pid =this.pSelector.getPartitionId(node.getId());
            if(StringUtils.isEmpty(pid)){
                continue;
            }
            String secondId = secondIds.getSecondId(pid,storageId);
            if(StringUtils.isEmpty(secondId)){
                continue;
            }
            node.setSecondId(secondId);
            duplicateNodes.add(node);
        }
        return duplicateNodes.isEmpty() ? new DuplicateNode[0] : duplicateNodes.toArray(new DuplicateNode[duplicateNodes.size()]);
    }
}
