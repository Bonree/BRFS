package com.bonree.brfs.duplication.coordinator.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.ServiceIdBuilder;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePicker;
import com.bonree.brfs.duplication.service.Service;
import com.bonree.brfs.duplication.service.ServiceManager;
import com.bonree.brfs.duplication.service.ServiceStateListener;
import com.bonree.brfs.duplication.utils.JsonUtils;
import com.bonree.brfs.duplication.utils.PooledThreadFactory;

public class ZookeeperFileCoordinator implements FileCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperFileCoordinator.class);
	
	private static final String COORDINATOR_ROOT = "fileCoordinator";
	private static final String COORDINATOR_FILENODES = "fileNodes";
	private static final String COORDINATOR_SINK = "fileSink";
	private static final String COORDINATOR_LEADER = "leader";
	
	private static final int SERVICE_INVALID_SECONDS = 1800;
	
	private static final String DUPLICATE_SERVICE_GROUP = "duplicate_group";
	
	private ServiceManager serviceManager;
	
	private CuratorFramework client;
	private LeaderSelector selector;
	private AtomicBoolean isLeader = new AtomicBoolean(false);
	
	private Service selfService = new Service();
	private PathChildrenCache sinkWatcher;
	
	private FilePicker picker;
	
	public ZookeeperFileCoordinator(CuratorFramework client, ServiceManager serviceManager) {
		this.client = client;
		this.selector = new LeaderSelector(client, ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_LEADER), new CoordinatorLeaderListener());
		this.serviceManager = serviceManager;
	}
	
	@Override
	public void start() throws Exception {
		client.createContainers(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES));
		client.createContainers(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK));
		
		selector.autoRequeue();
		selector.start();
		serviceManager.addServiceStateListener(DUPLICATE_SERVICE_GROUP, new DuplicateServiceStateListener());
		
		selfService = new Service();
		selfService.setServiceGroup(DUPLICATE_SERVICE_GROUP);
		selfService.setServiceId(ServiceIdBuilder.getServiceId());//TODO
		selfService.setHost("localhost");//TODO
		selfService.setPort(8899);//TODO
		
		watchSink();
		
		serviceManager.registerService(selfService);
	}
	
	@Override
	public void stop() throws Exception {
		CloseableUtils.closeQuietly(sinkWatcher);
		CloseableUtils.closeQuietly(selector);
	}
	
	private void watchSink() throws Exception {
		sinkWatcher = new PathChildrenCache(client, ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK, selfService.getServiceId()), false);
		sinkWatcher.getListenable().addListener(new FileNodeListener());
		sinkWatcher.start();
	}
	
	private void removeSink() throws Exception {
		client.delete().deletingChildrenIfNeeded().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK, selfService.getServiceId()));
	}
	
	@Override
	public void setFilePicker(FilePicker picker) {
		this.picker = picker;
	}

	@Override
	public boolean publish(FileNode node) {
		String fileNodePath = ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES, node.getName());
		Stat fileNodeStat = null;
		try {
			fileNodeStat = client.checkExists().forPath(fileNodePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(fileNodeStat != null) {
			return false;
		}
		
		String result = null;
		try {
			result = client.create().forPath(fileNodePath, JsonUtils.toJsonBytes(node));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(result == null) {
			return false;
		}
		
		return true;
	}

	@Override
	public boolean release(String fileName) {
		String fileNodePath = ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES, fileName);
		try {
			client.delete().forPath(fileNodePath);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	private List<FileNode> searchFileNodebyService(String serviceId) {
		List<FileNode> fileNodes = new ArrayList<FileNode>();
		try {
			List<String> fileNodeNames = client.getChildren().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES));
			for(String fileName : fileNodeNames) {
				FileNode node = JsonUtils.toObject(client.getData().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES, fileName)), FileNode.class);
				if(node.getServiceId().equals(serviceId)) {
					fileNodes.add(node);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return fileNodes;
	}
	
	private List<String> availableSink() {
		List<String> sinks = new ArrayList<String>();
		try {
			sinks.addAll(client.getChildren().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK)));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return sinks;
	}
	
	private void updateFileNode(FileNode node) {
		try {
			//先更新文件节点所属的服务名
			client.setData().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_FILENODES, node.getName()), JsonUtils.toJsonBytes(node));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void designateFileNodeToService(String serviceId, FileNode node) {
		node.setServiceId(serviceId);
		updateFileNode(node);
		
		//在Sink中放入分配的文件名
		try {
			client.create().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK, node.getServiceId(), node.getName()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class FileNodeListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client,
				PathChildrenCacheEvent event) throws Exception {
			LOG.info("EVENT--{}", event);
			ChildData data = event.getData();
			if(data != null) {
				switch (event.getType()) {
				case CHILD_ADDED:
					data.getPath();
					break;
				default:
					break;
				}
			}
		}
		
	}
	
	private class DuplicateServiceStateListener implements ServiceStateListener {
		private Map<String, ScheduledFuture<?>> tasks = new HashMap<String, ScheduledFuture<?>>();
		
		private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("dup_service_handler"));

		@Override
		public void serviceAdded(Service service) {
		}

		@Override
		public void serviceRemoved(Service service) {
			ScheduledFuture<?> task = exec.schedule(new FileNodeDesignator(service), SERVICE_INVALID_SECONDS, TimeUnit.SECONDS);
			tasks.put(service.getServiceId(), task);
		}
		
	}
	
	private class FileNodeDesignator implements Runnable {
		private Random random = new Random();
		private Service downService;
		
		public FileNodeDesignator(Service service) {
			this.downService = service;
		}

		@Override
		public void run() {
			try {
				Participant leader = selector.getLeader();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
			if(!isLeader.get()) {
				return;
			}
			
			try {
				client.delete().deletingChildrenIfNeeded().forPath(ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK, selfService.getServiceId()));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			List<String> availableSinks  = availableSink();
			for(FileNode node : searchFileNodebyService(downService.getServiceId())) {
				designateFileNodeToService(availableSinks.get(random.nextInt(availableSinks.size())), node);
			}
		}
		
	}
	
	private class CoordinatorLeaderListener implements LeaderSelectorListener {

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			System.out.println("leader is me!");
			
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
}
