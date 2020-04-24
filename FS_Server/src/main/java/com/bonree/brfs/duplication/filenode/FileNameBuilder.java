package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.identification.VirtualServerID;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNameBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FileNameBuilder.class);

    public static String createFile(VirtualServerID idManager, StorageRegion storageRegion, DuplicateNode[] duplicateNodes) {
        StringBuilder builder = new StringBuilder();
        builder.append(UUID.randomUUID().toString().replaceAll("-", ""));

        List<String> ids = new ArrayList<String>();
        for (DuplicateNode node : duplicateNodes) {
            ids.add(node.getId());
            builder.append('_').append(node.getSecondId());
        }

        int virtualIdCount = storageRegion.getReplicateNum() - duplicateNodes.length;
        if (virtualIdCount > 0) {
            for (String virtualId : idManager.getVirtualID(storageRegion.getId(), virtualIdCount, ids)) {
                LOG.info("get virtual id---{}", virtualId);
                builder.append('_').append(virtualId);
            }
        }

        return builder.toString();
    }
}
