package com.bonree.brfs.duplication.coordinator.zk;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeSinkSelector;

/**
 * 随机选择
 * 
 * @author yupeng
 *
 */
public class RandomFileNodeServiceSelector implements FileNodeSinkSelector {
	private static Random random = new Random();

	@Override
	public Service selectWith(FileNode fileNode, List<Service> serviceList) {
		if(serviceList.isEmpty()) {
			return null;
		}
		
		return serviceList.get(random.nextInt(serviceList.size()));
	}

}
