package com.bonree.brfs.common.service;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.common.service.impl.DefaultServiceManager;

public class TestServiceManager {
	private static final String zk_address = "192.168.101.86:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zk_address, 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		ServiceManager sm = new DefaultServiceManager(client.usingNamespace("test_c"));
		sm.start();
		
		sm.addServiceStateListener("ss_g", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void serviceAdded(Service service) {
				System.out.println("##########################################before add " + service);
			}
		});
		
		sm.registerService(new Service("ss_1", "ss_g", "localhost", 999));
		
		Thread.sleep(1000);
		
		sm.addServiceStateListener("ss_g", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void serviceAdded(Service service) {
				System.out.println("##########################################after add " + service);
			}
		});
		
		sm.addServiceStateListener("ss_g", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void serviceAdded(Service service) {
				System.out.println("##########################################after_2 add " + service);
			}
		});
		
		sm.registerService(new Service("ss_2", "ss_g", "localhost", 1222));
		
		synchronized (sm) {
			sm.wait();
		}
	}

}
