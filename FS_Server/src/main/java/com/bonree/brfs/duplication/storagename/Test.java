package com.bonree.brfs.duplication.storagename;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Test {

	private static final String zk_address = "122.11.47.17:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, retryPolicy);
		client.start();
		
		
		StorageNameManager snm = new DefaultStorageNameManager(client.usingNamespace("test"));
		snm.start();
		
//		Thread.sleep(2000);
		
		StorageNameNode node = snm.createStorageName("sn_1", 2, 100);
		System.out.println("create node--" + node);
		
		StorageNameNode node2 = snm.createStorageName("sn_2", 2, 100);
		System.out.println("create node--" + node2);
		
		System.out.println("find node--" + snm.findStorageName("sn_1"));
		
//		Thread.sleep(1000);
		
		System.out.println("delete--" + snm.removeStorageName("sn_1"));
//		System.out.println("find node--" + snm.findStorageName("sn_1"));
		
		System.out.println("update--" + snm.updateStorageName("sn_2", 110));
		
		synchronized (client) {
			client.wait();
		}
	}

}
