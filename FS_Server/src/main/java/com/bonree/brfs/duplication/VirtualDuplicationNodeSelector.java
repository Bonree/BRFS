package com.bonree.brfs.duplication;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.server.identification.ServerIDManager;

public class VirtualDuplicationNodeSelector implements DuplicationNodeSelector {
	private ServiceManager serviceManager;
	private ServerIDManager idManager;
	private Random rand = new Random();
	
	public VirtualDuplicationNodeSelector(ServiceManager serviceManager, ServerIDManager idManager) {
		this.serviceManager = serviceManager;
		this.idManager = idManager;
	}

	@Override
	public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		int serviceCount = serviceList.size();
		if(serviceCount == 0) {
			return new DuplicateNode[0];
		}
		
		DuplicateNode[] nodes = new DuplicateNode[nums];
		int index = rand.nextInt(serviceCount);
		
		int nodeIndex = 0;
		//分配真实服务节点
		for(; nodeIndex < serviceCount; nodeIndex++) {
			Service service = serviceList.get(index);
			
			nodes[nodeIndex] = new DuplicateNode();
			nodes[nodeIndex].setGroup(service.getServiceGroup());
			nodes[nodeIndex].setId(service.getServiceId());
			
			index = (index + 1) % serviceCount;
		}
		
		//分配虚拟服务节点
		int virtualCount = nums - nodeIndex;
		for(String virtualId : idManager.getVirtualServerID(storageId, virtualCount)) {
			nodes[nodeIndex] = new DuplicateNode();
			nodes[nodeIndex].setGroup(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP);
			nodes[nodeIndex].setId(virtualId);
			
			nodeIndex++;
		}
		
		return nodes;
	}

}
