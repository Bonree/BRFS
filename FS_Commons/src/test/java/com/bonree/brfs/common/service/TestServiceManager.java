package com.bonree.brfs.common.service;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.common.service.impl.DefaultServiceManager;

public class TestServiceManager {
	private static final String zk_address = "122.11.47.17:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		ServiceManager sm = new DefaultServiceManager(client.usingNamespace("brfs"));
		sm.start();
		
		List<Service> slist = sm.getServiceListByGroup("a");
		sm.addServiceStateListener("a", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void serviceAdded(Service service) {
				// TODO Auto-generated method stub
				
			}
		});
	}

}
