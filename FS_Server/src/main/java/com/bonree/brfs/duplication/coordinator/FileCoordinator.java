package com.bonree.brfs.duplication.coordinator;

/**
 * 文件节点协调器
 * 
 * @author yupeng
 *
 */
public class FileCoordinator {
	private FileNodeStorer storer;
	private FileNodeSinkManager sinkManager;
	
	public FileCoordinator(FileNodeStorer storer, FileNodeSinkManager sinkManager) {
		this.storer = storer;
		this.sinkManager = sinkManager;
	}
	
	/**
	 * 向文件节点仓库存储新节点
	 * 
	 * @param fileNode
	 * @throws Exception
	 */
	public void store(FileNode fileNode) throws Exception {
		storer.save(fileNode);
	}
	
	/**
	 * 删除仓库中的文件节点
	 * 
	 * @param fileNode
	 * @throws Exception
	 */
	public void delete(FileNode fileNode) throws Exception {
		storer.delete(fileNode.getName());
	}
	
	/**
	 * 注册一个文件接收器
	 * 
	 * @param sink
	 * @throws Exception
	 */
	public void addFileNodeSink(FileNodeSink sink) throws Exception {
		sinkManager.registerFileNodeSink(sink);
	}
	
	public void setFileNodeCleanListener(FileNodeInvalidListener listener) {
		storer.setFileNodeInvalidListener(listener);
	}
}
