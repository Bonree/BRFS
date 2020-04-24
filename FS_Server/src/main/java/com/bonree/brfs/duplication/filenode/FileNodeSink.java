package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.duplication.storageregion.StorageRegion;

/**
 * 接受转移文件的节点槽
 *
 * @author yupeng
 */
public interface FileNodeSink {

    /**
     * 获取节点槽所属的数据库
     *
     * @return
     */
    StorageRegion getStorageRegion();

    /**
     * 当有文件节点放入槽中时，此方法会被调用
     *
     * @param fileNode
     */
    void received(FileNode fileNode);
}
