package com.bonree.brfs.common.service;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.common.service.impl.DefaultServiceManager;

public class TestServiceManager {
	private static final String zk_address = "192.168.107.13:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		ServiceManager sm = new DefaultServiceManager(client.usingNamespace("test_c"));
		sm.start();
		
		sm.registerService(new Service("ss_1", "ss_g", "localhost", 999));
		
		synchronized (sm) {
			sm.wait();
		}
	}

}
