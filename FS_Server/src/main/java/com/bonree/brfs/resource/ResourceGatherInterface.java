package com.bonree.brfs.resource;

import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.resource.vo.ResourceModel;

/**
 * 资源采集接口
 */
public interface ResourceGatherInterface {
    ResourceModel gatherClusterResource() throws Exception;

    NodeSnapshotInfo gatherSnapshot() throws Exception;
}
