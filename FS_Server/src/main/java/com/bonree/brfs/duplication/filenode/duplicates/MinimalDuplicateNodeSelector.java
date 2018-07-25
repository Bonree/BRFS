package com.bonree.brfs.duplication.filenode.duplicates;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;

public class MinimalDuplicateNodeSelector implements DuplicateNodeSelector {
	private ServiceManager serviceManager;
	private DiskNodeConnectionPool connectionPool;
	private Random rand = new Random();
	
	public MinimalDuplicateNodeSelector(ServiceManager serviceManager, DiskNodeConnectionPool connectionPool) {
		this.serviceManager = serviceManager;
		this.connectionPool = connectionPool;
	}

	@Override
	public DuplicateNode[] getDuplicationNodes(int storageId, int nums) {
		List<Service> serviceList = serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
		if(serviceList.isEmpty()) {
			return new DuplicateNode[0];
		}
		
		List<DuplicateNode> nodes = new ArrayList<DuplicateNode>();
		while(!serviceList.isEmpty() && nodes.size() < nums) {
			Service service = serviceList.remove(rand.nextInt(serviceList.size()));
			
			DuplicateNode node = new DuplicateNode(service.getServiceGroup(), service.getServiceId());
			DiskNodeConnection conn = connectionPool.getConnection(node.getGroup(), node.getId());
			if(conn == null || conn.getClient() == null || !conn.getClient().ping()) {
				continue;
			}
			
			nodes.add(node);
		}
		
		DuplicateNode[] result = new DuplicateNode[nodes.size()];
		return nodes.toArray(result);
	}

}
