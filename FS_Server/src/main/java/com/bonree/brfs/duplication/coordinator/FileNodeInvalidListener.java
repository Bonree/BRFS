package com.bonree.brfs.duplication.coordinator;

/**
 * 文件节点无效化处理接口
 * 
 * @author yupeng
 *
 */
public interface FileNodeInvalidListener {
	/**
	 * 对文件节点进行清理
	 */
	void invalid();
}
