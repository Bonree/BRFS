package com.bonree.brfs.duplication.service;

import java.util.Random;
import java.util.UUID;

import javax.sql.rowset.serial.SerialArray;

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
		client.blockUntilConnected();

		ServiceManager sm = new DefaultServiceManager(client.usingNamespace("test"));
		sm.start();
		
		sm.addServiceStateListener("g_1", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				System.out.println("remove--" + service);
			}
			
			@Override
			public void serviceAdded(Service service) {
				System.out.println("add--" + service);
			}
		});
		
		Service s = new Service(UUID.randomUUID().toString(), "g_1", "122", 1);
		sm.registerService(s);
		
		synchronized (client) {
			client.wait();
		}
	}

}
