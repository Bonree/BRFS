package com.bonree.brfs.duplication.coordinator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 文件节点协调器
 * 
 * @author yupeng
 *
 */
public class FileCoordinator {
	private CuratorFramework client;
	private FileNodeStorer storer;
	private FileNodeSinkManager sinkManager;
	
	public FileCoordinator(CuratorFramework client, FileNodeStorer storer, FileNodeSinkManager sinkManager) {
		this.client = client;
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
		client.getConnectionStateListenable().addListener(new ZkConnectionStateListener(listener));
	}
	
	/**
	 * 
	 * 对Zookeeper的连接状态进行监听
	 * 
	 * @author yupeng
	 *
	 */
	private class ZkConnectionStateListener implements ConnectionStateListener {
		private FileNodeInvalidListener listener;
		
		public ZkConnectionStateListener(FileNodeInvalidListener listener) {
			this.listener = listener;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			if(!newState.isConnected() && listener != null) {
				//如果连接断开则清理当前服务维护的所有文件节点
				listener.invalid();
			}
		}
		
	}
}
