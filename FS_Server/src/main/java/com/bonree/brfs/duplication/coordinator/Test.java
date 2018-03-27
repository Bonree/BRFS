package com.bonree.brfs.duplication.coordinator;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.duplication.coordinator.zk.ZookeeperFileCoordinator;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.utils.JsonUtils;

public class Test {
	private static final String zk_address = "122.11.47.17:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		StorageNameManager snm = new DefaultStorageNameManager(client.usingNamespace("test"));
		snm.start();
		
		ZookeeperFileCoordinator coordinator = new ZookeeperFileCoordinator(client.usingNamespace("test"), null);
		coordinator.start();
		
		Thread.sleep(3000);
		
		FileNode node = new FileNode();
		node.setName("file_node_1");
		node.setStorageName("sn_1");
		node.setServiceId("me");
		node.setDuplicates(new int[]{1 , 2, 3});
		System.out.println("publish---" + coordinator.publish(node));
		
//		System.out.println("release--" + coordinator.release(node.getName()));
		
		
		synchronized (client) {
			client.wait();
		}
	}

}
