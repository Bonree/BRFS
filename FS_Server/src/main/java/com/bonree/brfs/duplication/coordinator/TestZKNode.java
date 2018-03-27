package com.bonree.brfs.duplication.coordinator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.nodes.PersistentNodeListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class TestZKNode {
	private static final String zk_address = "122.11.47.17:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		PathChildrenCache cache = new PathChildrenCache(client.usingNamespace("test"), "/fileCoordinator/big", true);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
					throws Exception {
				System.out.println("---" + event);
				ChildData data = event.getData();
				if(data != null) {
					switch (event.getType()) {
					case CHILD_ADDED:
						System.out.println("###PATH-" + data.getPath());
						break;
					default:
						break;
					}
				}
			}
		});
		cache.start();
		
		PersistentNode node = new PersistentNode(client.usingNamespace("test"), CreateMode.EPHEMERAL, false, "/fileCoordinator/temp-1", "node1".getBytes());
		node.getListenable().addListener(new PersistentNodeListener() {
			
			@Override
			public void nodeCreated(String path) throws Exception {
				System.out.println("node1--created:" + path);
			}
		});
		node.start();
		
		PersistentNode node2 = new PersistentNode(client.usingNamespace("test"), CreateMode.EPHEMERAL, false, "/fileCoordinator/temp-1", "node2".getBytes());
		node2.getListenable().addListener(new PersistentNodeListener() {
			
			@Override
			public void nodeCreated(String path) throws Exception {
				System.out.println("node2--created:" + path);
			}
		});
		node2.start();
		
		Thread.sleep(2000);
		node2.close();
		
//		node.close();
		
		synchronized (node) {
			node.wait();
		}
	}

}
