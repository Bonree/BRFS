package com.bonree.brfs.duplication.coordinator.zk;

import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeServiceSelector;

public class RandomFileNodeServiceSelector implements FileNodeServiceSelector {
	private static Random random = new Random();

	@Override
	public Service selectWith(FileNode fileNode, List<Service> services) {
		return services.get(random.nextInt(services.size()));
	}

}
