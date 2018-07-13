package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.duplication.storagename.StorageNameNode;

/**
 * 接受转移文件的节点槽
 * 
 * @author yupeng
 *
 */
public interface FileNodeSink {
	
	/**
	 * 获取节点槽所属的数据库
	 * 
	 * @return
	 */
	StorageNameNode getStorageRegion();
	
	/**
	 * 当有文件节点放入槽中时，此方法会被调用
	 * 
	 * @param fileNode
	 */
	void received(FileNode fileNode);
}
