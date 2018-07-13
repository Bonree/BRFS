package com.bonree.brfs.duplication.filenode;

import java.util.List;

/**
 * 文件节点的存储中心
 * 
 * @author yupeng
 *
 */
public interface FileNodeStorer {
	/**
	 * 保存文件节点到仓库
	 * 
	 * @param fileNode
	 * @throws Exception
	 */
	void save(FileNode fileNode) throws Exception;
	
	/**
	 * 从仓库删除文件节点
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	void delete(String fileName) throws Exception;
	
	/**
	 * 获取指定文件名对应的文件节点
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	FileNode getFileNode(String fileName) throws Exception;
	
	/**
	 * 更新文件节点信息
	 * 
	 * @param fileNode
	 * @throws Exception
	 */
	void update(FileNode fileNode) throws Exception;
	
	/**
	 * 列举当前仓库中所有的文件节点
	 * 
	 * @param filter 文件节点过滤器；如果不需要过滤，可以为null
	 * @return
	 */
	List<FileNode> listFileNodes();
	
	/**
	 * 列举当前仓库中的文件节点，可以使用过滤器对
	 * 节点进行筛选
	 * 
	 * @param filter 文件节点过滤器；如果不需要过滤，可以为null
	 * @return
	 */
	List<FileNode> listFileNodes(FileNodeFilter filter);
}
