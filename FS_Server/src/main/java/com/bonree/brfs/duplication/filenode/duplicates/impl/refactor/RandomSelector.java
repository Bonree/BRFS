package com.bonree.brfs.duplication.filenode.duplicates.impl.refactor;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.PartitionNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.MinimalDuplicateNodeSelector;
import com.bonree.brfs.identification.SecondIdsInterface;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class RandomSelector extends MinimalDuplicateNodeSelector {
    private PartitionNodeSelector nodeSelector;
    private SecondIdsInterface secondIds;

    public RandomSelector(ServiceManager serviceManager,
                          DiskNodeConnectionPool connectionPool,
                          PartitionNodeSelector nodeSelector,
                          SecondIdsInterface secondIds) {
        super(serviceManager, connectionPool);
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
        return duplicateNodes.isEmpty() ? new DuplicateNode[0] : duplicateNodes.toArray(new DuplicateNode[duplicateNodes.size()]);
    }
}
