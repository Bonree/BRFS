package com.bonree.brfs.discover;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;

public class TestService {
	private static final int ID = 3;

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.101.86:2181", 3000, 3000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		ServiceManager sm = new DefaultServiceManager(client.usingNamespace("test"));
		sm.start();
		
		Service s = new Service();
		s.setServiceGroup("group");
		s.setServiceId("ser_" + ID);
		s.setHost("local");
		
		sm.registerService(s);
		Service tmp = sm.getServiceById(s.getServiceGroup(), s.getServiceId());
		System.out.println(tmp);
		
		sm.addServiceStateListener("group", new ServiceStateListener() {
			
			@Override
			public void serviceRemoved(Service service) {
				System.out.println("remove--" + service.getServiceId());
			}
			
			@Override
			public void serviceAdded(Service service) {
				System.out.println("add--" + service.getServiceId());
			}
		});
		System.out.println(sm.getServiceById(s.getServiceGroup(), s.getServiceId()));
	}

}
