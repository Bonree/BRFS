package com.bonree.brfs.duplication.coordinator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

public class FileCenter {
	private static final String zk_address = "122.11.47.17:2181";
	
	private static final String ROOT = "test";
	private static final String DUPS = "/dups";
	
	private static int id;

	public static void main(String[] args) {
		id = new Random().nextInt(10);
		System.out.println("id = " + id);
		
		System.out.println("hahahahha");
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder().namespace(ROOT).connectString(zk_address).retryPolicy(retryPolicy).build();
		client.start();
		
		try {
			Stat stat = client.checkExists().forPath(DUPS);
			System.out.println("stat =" + stat);
			if(stat == null) {
				System.out.println("create--" + client.create().forPath(DUPS));
			}
			
			ExecutorService pool = Executors.newFixedThreadPool(5);
			
			PathChildrenCache pathCache = new PathChildrenCache(client, DUPS, true, false, pool);
			pathCache.getListenable().addListener(new PathNodeListener());
			pathCache.start();
			
//			TreeCache cache = new TreeCache(client, DUPS);
//			cache.getListenable().addListener(new TreeNodeListener(), pool);
//			cache.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		synchronized (client) {
			try {
				client.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		client.close();
	}
	
	private static final class PathNodeListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			ChildData data = event.getData();
			if(data != null) {
				switch (event.getType()) {
				case CHILD_ADDED:
					System.out.println("ADD " + data.getPath() + ", " + new String(data.getData()));
					LeaderSelector selector = new LeaderSelector(client, data.getPath(), new LeaderSelectorListener() {
						
						@Override
						public void stateChanged(CuratorFramework client, ConnectionState newState) {
							System.out.println("state changed");
						}
						
						@Override
						public void takeLeadership(CuratorFramework client) throws Exception {
							System.out.println("take---" + data.getPath());
							client.setData().forPath(data.getPath(), ("S_" + id).getBytes());
							Thread.sleep(10000);
						}
					});
					selector.autoRequeue();
					selector.start();
					break;
				case CHILD_REMOVED:
					System.out.println("REMOVE " + data.getPath() + ", " + new String(data.getData()));
					break;
				case CHILD_UPDATED:
					System.out.println("UPDATE " + data.getPath() + ", " + new String(data.getData()));
					break;
				default:
					break;
				}
			}
		}
		
	}
	
	private static final class TreeNodeListener implements TreeCacheListener {

		@Override
		public void childEvent(CuratorFramework client, TreeCacheEvent event)
				throws Exception {
			ChildData data = event.getData();
			if(data != null) {
				switch (event.getType()) {
				case NODE_ADDED:
					System.out.println("ADD " + data.getPath() + ", " + new String(data.getData()));
					LeaderSelector selector = new LeaderSelector(client, data.getPath(), new LeaderSelectorListener() {
						
						@Override
						public void stateChanged(CuratorFramework client, ConnectionState newState) {
							System.out.println("state changed");
						}
						
						@Override
						public void takeLeadership(CuratorFramework client) throws Exception {
							System.out.println("leader take!");
							Thread.sleep(10000);
						}
					});
					selector.start();
					break;
				case NODE_REMOVED:
					System.out.println("REMOVE " + data.getPath() + ", " + new String(data.getData()));
					break;
				case NODE_UPDATED:
					System.out.println("UPDATE " + data.getPath() + ", " + new String(data.getData()));
					break;
				default:
					break;
				}
			}
		}
		
	}
}
