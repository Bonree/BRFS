package com.bonree.brfs.duplication;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class DefaultDuplicationNodeSelector implements DuplicationNodeSelector {
	private ServiceManager serviceManager;
	private Random rand = new Random();
	
	public DefaultDuplicationNodeSelector(ServiceManager serviceManager) {
		this.serviceManager = serviceManager;
	}

	@Override
	public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME));
		if(serviceList.isEmpty()) {
			return new DuplicateNode[0];
		}
		
		int n = Math.min(nums, serviceList.size());
		DuplicateNode[] nodes = new DuplicateNode[n];
		int index = rand.nextInt(n);
		
		for(int i = 0; i < n; i++) {
			Service service = serviceList.get(index);
			
			nodes[i] = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
			
			index = (index + 1) % n;
		}
		
		return nodes;
	}

}
