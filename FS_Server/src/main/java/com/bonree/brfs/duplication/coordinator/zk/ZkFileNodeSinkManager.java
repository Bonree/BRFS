package com.bonree.brfs.duplication.coordinator.zk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeSink;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkSelector;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.recovery.DelayedRecoveryHandler;

public class ZkFileNodeSinkManager implements FileNodeSinkManager {
	private static final Logger LOG = LoggerFactory.getLogger(ZkFileNodeSinkManager.class);

	private CuratorFramework client;
	private LeaderSelector selector;

	private ServiceManager serviceManager;

	private PathChildrenCache sinkWatcher;

	private AtomicBoolean isLeader = new AtomicBoolean(false);
	
	private FileNodeDistributor distributor;
	private static final String DISTRIBUTOR_THREAD_NAME = "file_distributor";
	private ExecutorService threadPool = Executors.newFixedThreadPool(2, new PooledThreadFactory(DISTRIBUTOR_THREAD_NAME));
	
	private DelayedRecoveryHandler recoveryHandler;

	public ZkFileNodeSinkManager(CuratorFramework client,
			ServiceManager serviceManager,
			FileNodeStorer storer,
			FileNodeSinkSelector selector) {
		this.client = client;
		this.serviceManager = serviceManager;
		this.distributor = new FileNodeDistributor(client, storer, serviceManager, selector);
		this.selector = new LeaderSelector(client, ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT, ZkFileCoordinatorPaths.COORDINATOR_LEADER),
				new SinkManagerLeaderListener());
	}

	@Override
	public void start() throws Exception {
		selector.autoRequeue();
		selector.start();

		// 监听副本管理服务的状态
		serviceManager.addServiceStateListener(
				ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, distributor);
	}

	@Override
	public void stop() throws Exception {
		serviceManager.removeServiceStateListener(
				ServerConfig.DEFAULT_DUPLICATION_SERVICE_GROUP, distributor);
		selector.close();
		threadPool.shutdown();
	}

	@Override
	public void registerFileNodeSink(FileNodeSink sink) throws Exception {
		//PathChildrenCache会自动创建sink节点所在的路径
		sinkWatcher = new PathChildrenCache(client, ZkFileCoordinatorPaths.buildSinkPath(sink.getService()), true);
		sinkWatcher.getListenable().addListener(new SinkNodeListener(sink));

		sinkWatcher.start();
	}

	/**
	 * Leader选举结果监听类
	 * 
	 * @author yupeng
	 *
	 */
	private class SinkManagerLeaderListener implements LeaderSelectorListener {

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			if(!newState.isConnected()) {
				synchronized (isLeader) {
					distributor.quit();
					isLeader.set(false);
					isLeader.notifyAll();
				}
			}
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			LOG.info("I am leader!");
			isLeader.set(true);
			
			try {
				threadPool.submit(distributor);
				threadPool.submit(recoveryHandler);
				
				synchronized (isLeader) {
					if(isLeader.get()) {
						isLeader.wait();
					}
				}
			} finally {
				isLeader.set(false);
			}
		}

	}

	/**
	 * Sink中文件节点变化情况监听类
	 * 
	 * @author yupeng
	 *
	 */
	private class SinkNodeListener implements PathChildrenCacheListener {
		private FileNodeSink sink;

		public SinkNodeListener(FileNodeSink sink) {
			this.sink = sink;
		}

		@Override
		public void childEvent(CuratorFramework client,
				PathChildrenCacheEvent event) throws Exception {
			ChildData data = event.getData();
			if(data == null) {
				return;
			}
			
			LOG.info("EVENT--{}--{}", event.getType(), data.getPath());
			switch (event.getType()) {
			case CHILD_ADDED:
				FileNode fileNode = JsonUtils.toObject(data.getData(), FileNode.class);
				sink.fill(fileNode);
				//如果节点接受成功则删除sink中的节点
				client.delete().quietly().forPath(data.getPath());
				break;
			default:
				break;
			}
		}

	}
}
