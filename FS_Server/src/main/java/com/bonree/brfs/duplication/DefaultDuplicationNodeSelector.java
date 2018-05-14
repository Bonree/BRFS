package com.bonree.brfs.duplication;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class DefaultDuplicationNodeSelector implements DuplicationNodeSelector {
	private ServiceManager serviceManager;
	private Random rand = new Random();
	
	public DefaultDuplicationNodeSelector(ServiceManager serviceManager) {
		this.serviceManager = serviceManager;
	}

	@Override
	public DuplicateNode[] getDuplicationNodes(int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		int n = Math.min(nums, serviceList.size());
		DuplicateNode[] nodes = new DuplicateNode[n];
		int index = rand.nextInt(n);
		
		for(int i = 0; i < n; i++) {
			Service service = serviceList.get(index);
			
			System.out.println("#######################" + service);
			
			nodes[i] = new DuplicateNode();
			nodes[i].setGroup(service.getServiceGroup());
			nodes[i].setId(service.getServiceId());
			
			index = (index + 1) % n;
		}
		
		return nodes;
	}

}
