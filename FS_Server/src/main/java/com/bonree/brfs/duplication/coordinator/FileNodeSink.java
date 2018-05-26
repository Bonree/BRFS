package com.bonree.brfs.duplication.coordinator;

import com.bonree.brfs.common.service.Service;

/**
 * 接受转移文件的节点槽
 * 
 * @author yupeng
 *
 */
public interface FileNodeSink {
	/**
	 * 节点槽所属服务
	 * 
	 * @return
	 */
	Service getService();
	
	/**
	 * 当有文件节点放入槽中时，此方法会被调用
	 * 
	 * @param fileNode
	 */
	void fill(FileNode fileNode);
}
