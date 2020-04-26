package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.common.service.Service;
import java.util.List;

/**
 * 为需要转移的文件选择一个合适sink
 *
 * @author yupeng
 */
public interface FileNodeSinkSelector {
    /**
     * 为{@link FileNode}选择一个合适的sink进行转移
     *
     * @param fileNode
     * @param serviceList
     *
     * @return
     */
    Service selectWith(FileNode fileNode, List<Service> serviceList);
}
