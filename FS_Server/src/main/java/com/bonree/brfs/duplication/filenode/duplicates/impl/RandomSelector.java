package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.identification.SecondIdsInterface;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomSelector extends MinimalDuplicateNodeSelector {
    private static final Logger log = LoggerFactory.getLogger(RandomSelector.class);
    private PartitionNodeSelector nodeSelector;
    private SecondIdsInterface secondIds;

    public RandomSelector(
        ServiceManager serviceManager, PartitionNodeSelector nodeSelector,
        SecondIdsInterface secondIds, String dataGroup) {
        super(serviceManager, log, dataGroup);
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
            node.setSecondId(secondId);
            duplicateNodes.add(node);
        }
        log.info("last select duplicatnode {}", duplicateNodes);
        return duplicateNodes.isEmpty() ? new DuplicateNode[0] : duplicateNodes.toArray(new DuplicateNode[duplicateNodes.size()]);
    }
}
