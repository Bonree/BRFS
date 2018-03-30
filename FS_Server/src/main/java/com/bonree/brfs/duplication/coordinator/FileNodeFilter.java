package com.bonree.brfs.duplication.coordinator;

/**
 * 文件节点过滤接口
 * 
 * @author root
 *
 */
public interface FileNodeFilter {
	/**
	 * 是否通过过滤器
	 * 
	 * @param fileNode
	 * @return true则通过，false则抛弃
	 */
	boolean filter(FileNode fileNode);
}
