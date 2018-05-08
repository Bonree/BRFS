package com.bonree.brfs.duplication.coordinator.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeFilter;
import com.bonree.brfs.duplication.coordinator.FileNodeServiceSelector;
import com.bonree.brfs.duplication.coordinator.FileNodeSink;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkManager;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;

public class ZkFileNodeSinkManager implements FileNodeSinkManager {
	private static final Logger LOG = LoggerFactory.getLogger(ZkFileNodeSinkManager.class);

	private CuratorFramework client;
	private LeaderSelector selector;

	private ServiceManager serviceManager;
	private ServiceStateListener serviceStateListener;

	private FileNodeStorer fileStorer;
	private PathChildrenCache sinkWatcher;

	private AtomicBoolean isLeader = new AtomicBoolean(false);
	
	private FileNodeServiceSelector serviceSelector;

	public ZkFileNodeSinkManager(CuratorFramework client,
			ServiceManager serviceManager,
			FileNodeStorer storer,
			FileNodeServiceSelector selector) {
		this.client = client;
		this.serviceManager = serviceManager;
		this.fileStorer = storer;
		this.serviceStateListener = new DuplicateServiceStateListener();
		this.serviceSelector = selector;
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
				FileCoordinator.DUPLICATE_SERVICE_GROUP, serviceStateListener);
	}

	@Override
	public void stop() throws Exception {
		serviceManager.removeServiceStateListener(
				FileCoordinator.DUPLICATE_SERVICE_GROUP, serviceStateListener);
		selector.close();
	}

	@Override
	public void registerFileNodeSink(FileNodeSink sink) throws Exception {
		sinkWatcher = new PathChildrenCache(client, buildSinkPath(sink.getService()), true);
		sinkWatcher.getListenable().addListener(new SinkNodeListener(sink));

		sinkWatcher.start();
	}
	
	//构建Service的Sink路径
	private String buildSinkPath(Service service) {
		return ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_SINK,
				service.getServiceId());
	}
	
	//构建Sink中FileNode的路径
	private String buildSinkFileNodePath(FileNode node) {
		return ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_SINK,
				node.getServiceId(), node.getName());
	}

	/**
	 * 服务状态监听类
	 * 
	 * @author chen
	 *
	 */
	private class DuplicateServiceStateListener implements ServiceStateListener {
		private static final int SERVICE_INVALID_SECONDS = 3;
		private Map<String, ScheduledFuture<?>> tasks = new HashMap<String, ScheduledFuture<?>>();

		private ScheduledExecutorService exec = Executors
				.newSingleThreadScheduledExecutor(new PooledThreadFactory(
						"dup_service_handler"));

		@Override
		public void serviceAdded(Service service) {
			LOG.info("Service added#######{}", service.getServiceId());
			ScheduledFuture<?> task = null;
			synchronized (tasks) {
				task = tasks.get(service.getServiceId());
			}
			if(task == null || (task != null && task.cancel(false))) {
				//文件瓜分任务取消成功，文件将全部发送到原服务节点
				ThreadPoolUtil.commonPool().execute(new Runnable() {
					
					@Override
					public void run() {
						try {
							for (FileNode node : fileStorer.listFileNodes(new ServiceFileNodeFilter(service.getServiceId()))) {
								designateFileNodeToService(node, service);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
				});
			}
		}

		@Override
		public void serviceRemoved(Service service) {
			LOG.info("Service removed#######{}", service.getServiceId());
			synchronized (tasks) {
				ScheduledFuture<?> task = exec.schedule(new FileNodeDistributor(
						service), SERVICE_INVALID_SECONDS, TimeUnit.SECONDS);
				tasks.put(service.getServiceId(), task);
			}
		}

	}
	
	private void designateFileNodeToService(FileNode node, Service service) throws Exception {
		node.setServiceId(service.getServiceId());
		fileStorer.update(node);

		// 在Sink中放入分配的文件名
		client.create().forPath(buildSinkFileNodePath(node), JsonUtils.toJsonBytes(node));
	}

	/**
	 * 把已消失的Service管理的FileNode分配到其他可用Service中，保证
	 * 文件节点可以继续写入
	 * 
	 * @author chen
	 *
	 */
	private class FileNodeDistributor implements Runnable {
		private Service downService;

		public FileNodeDistributor(Service service) {
			this.downService = service;
		}

		@Override
		public void run() {
			if (!isLeader.get()) {
				//我不是Leader，没权利管这事，告辞！
				return;
			}

			try {
				client.delete()
				      .quietly()
					  .deletingChildrenIfNeeded()
					  .forPath(buildSinkPath(downService));
			} catch (Exception e) {
				LOG.warn("Can not delete the sink of crushed service[{}]", downService.getServiceId(), e);
			}

			//获取当前可用的服务列表
			List<Service> availableServices = serviceManager.getServiceListByGroup(downService.getServiceGroup());
			try {
				for (FileNode node : fileStorer.listFileNodes(new ServiceFileNodeFilter(downService.getServiceId()))) {
					designateFileNodeToService(node, serviceSelector.selectWith(node, availableServices));
				}
			} catch (Exception e) {
				//TODO handle the Exception correctly
				LOG.error("Unhandle Exception", e);
			}
		}

	}

	/**
	 * 过滤只属于指定Service的FileNode
	 * 
	 * @author chen
	 *
	 */
	private class ServiceFileNodeFilter implements FileNodeFilter {
		private String serviceId;

		public ServiceFileNodeFilter(String serviceId) {
			this.serviceId = serviceId;
		}

		@Override
		public boolean filter(FileNode fileNode) {
			return fileNode.getServiceId().equals(serviceId);
		}

	}

	/**
	 * Leader选举结果监听类
	 * 
	 * @author chen
	 *
	 */
	private class SinkManagerLeaderListener implements
			LeaderSelectorListener {

		@Override
		public void stateChanged(CuratorFramework client,
				ConnectionState newState) {
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			LOG.info("I am leader!");

			isLeader.set(true);
			try {

				synchronized (isLeader) {
					isLeader.wait();
				}
			} finally {
				isLeader.set(false);
			}
		}

	}

	/**
	 * Sink中文件节点变化情况监听类
	 * 
	 * @author chen
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
			LOG.info("EVENT--{}--{}", event.getType(), data.getPath());
			if (data != null) {
				switch (event.getType()) {
				case CHILD_ADDED:
					FileNode fileNode = JsonUtils.toObject(data.getData(),
							FileNode.class);
					sink.fill(fileNode);

					client.delete().quietly().forPath(data.getPath());
					break;
				default:
					break;
				}
			}
		}

	}
}
