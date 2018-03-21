package com.br.duplication.coordinator;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;

import com.br.duplication.service.Service;
import com.br.duplication.service.ServiceManager;
import com.br.duplication.service.ServiceStateListener;
import com.br.duplication.storagename.StorageNameManager;
import com.br.duplication.storagename.StorageNameNode;
import com.br.duplication.utils.PooledThreadFactory;
import com.br.duplication.utils.ProtoStuffUtils;

public class ZookeeperFileCoordinator implements FileCoordinator {
	private static final String STORAGE_NAME_ROOT = "storageNames";
	
	private static final String DUPLICATE_SERVICE_GROUP = "duplicate";
	
	private static final String COORDINATOR_ROOT = "coordinator";
	private static final String COORDINATOR_LEADER = "leader";
	
	private static final int SERVICE_INVALID_SECONDS = 1800;
	
	private ServiceManager serviceManager;
	private StorageNameManager storageNameManager;
	
	private CuratorFramework client;
	private LeaderSelector selector;
	private AtomicBoolean isLeader = new AtomicBoolean(false);
	
	public ZookeeperFileCoordinator(CuratorFramework client) {
		this.client = client;
		this.selector = new LeaderSelector(client, ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_LEADER), new CoordinatorLeaderListener());
	}
	
	public ZookeeperFileCoordinator(CuratorFramework client, ServiceManager serviceManager, StorageNameManager storageNameManager) {
		this.client = client;
		this.selector = new LeaderSelector(client, ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_LEADER), new CoordinatorLeaderListener());
		this.serviceManager = serviceManager;
		this.storageNameManager = storageNameManager;
	}
	
	@Override
	public void start() throws Exception {
		selector.start();
//		serviceManager.addServiceStateListener(DUPLICATE_SERVICE_GROUP, new DuplicateServiceStateListener());
	}
	
	@Override
	public void stop() {
		CloseableUtils.closeQuietly(selector);
	}

	@Override
	public boolean publish(FileNode info) {
		StorageNameNode storageName = storageNameManager.findStorageName(info.getStorageId());
		if(storageName == null) {
			return false;
		}
		
		Stat storageNameStat = null;
		try {
			storageNameStat = client.checkExists().forPath(ZKPaths.makePath(STORAGE_NAME_ROOT, storageName.getName()));
		} catch (Exception e) {
		}
		
		if(storageNameStat == null) {
			return false;
		}
		
		String fileNodePath = ZKPaths.makePath(STORAGE_NAME_ROOT, storageName.getName(), info.getName());
		Stat fileNodeStat = null;
		try {
			fileNodeStat = client.checkExists().forPath(fileNodePath);
		} catch (Exception e) {
		}
		
		if(fileNodeStat != null) {
			return false;
		}
		
		String result = null;
		try {
			result = client.create().forPath(fileNodePath, ProtoStuffUtils.serialize(info.getFileNodeData()));
		} catch (Exception e) {
		}
		
		if(result == null) {
			return false;
		}
		
		return true;
	}

	@Override
	public void release(FileNode node) {
		// TODO Auto-generated method stub
		
	}
	
	private class DuplicateServiceStateListener implements ServiceStateListener {
		private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("dup_service_handler"));

		@Override
		public void serviceAdded(Service service) {
		}

		@Override
		public void serviceRemoved(Service service) {
			exec.schedule(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					return null;
				}
			}, SERVICE_INVALID_SECONDS, TimeUnit.SECONDS);
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
