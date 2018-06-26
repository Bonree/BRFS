package com.bonree.brfs.duplication;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.server.identification.ServerIDManager;

public class VirtualDuplicationNodeSelector implements DuplicationNodeSelector {
	private static final Logger LOG = LoggerFactory.getLogger(VirtualDuplicationNodeSelector.class);
	private ServiceManager serviceManager;
	private ServerIDManager idManager;
	private Random rand = new Random();
	
	public VirtualDuplicationNodeSelector(ServiceManager serviceManager, ServerIDManager idManager) {
		this.serviceManager = serviceManager;
		this.idManager = idManager;
	}

	@Override
	public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_SERVICE_GROUP_NAME));
		int serviceCount = serviceList.size();
		if(serviceCount == 0) {
			return new DuplicateNode[0];
		}
		
		DuplicateNode[] nodes = new DuplicateNode[nums];
		int index = rand.nextInt(serviceCount);
		
		int nodeIndex = 0;
		//分配真实服务节点
		for(; nodeIndex < serviceCount && nodeIndex < nums; nodeIndex++) {
			Service service = serviceList.get(index);
			
			nodes[nodeIndex] = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
			
			index = (index + 1) % serviceCount;
		}
		
		//分配虚拟服务节点
		int virtualCount = nums - nodeIndex;
		if(virtualCount > 0) {
			LOG.info("---get virtual id---{}", virtualCount);
			for(String virtualId : idManager.getVirtualServerID(storageId, virtualCount)) {
				LOG.info("virtual id---{}", virtualId);
				nodes[nodeIndex] = new DuplicateNode(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP, virtualId);
				
				nodeIndex++;
			}
		}
		
		return nodes;
	}

}
