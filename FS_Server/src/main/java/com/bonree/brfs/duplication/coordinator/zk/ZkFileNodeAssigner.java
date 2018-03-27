package com.bonree.brfs.duplication.coordinator.zk;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.duplication.coordinator.FileNodeAssigner;
import com.bonree.brfs.duplication.coordinator.FileNodeReceiver;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.service.ServiceManager;

public class ZkFileNodeAssigner implements FileNodeAssigner {
	private CuratorFramework client;
	private LeaderSelector selector;

	private ServiceManager serviceManager;

	private FileNodeReceiver receiver;
	private FileNodeStorer fileStorer;
	
	private AtomicBoolean isLeader = new AtomicBoolean(false);

	public ZkFileNodeAssigner(CuratorFramework client,
			ServiceManager serviceManager, FileNodeStorer storer) {
		this.client = client;
		this.serviceManager = serviceManager;
		this.fileStorer = storer;
		this.selector = new LeaderSelector(client, ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_LEADER),
				new FileNodeAssignerLeaderListener());
	}

	@Override
	public void setFileNodeReceiver(FileNodeReceiver receiver) {
		this.receiver = receiver;
	}

	private class FileNodeAssignerLeaderListener implements
			LeaderSelectorListener {

		@Override
		public void stateChanged(CuratorFramework client,
				ConnectionState newState) {
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
