package com.bonree.brfs.duplication.coordinator.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeFilter;

/**
 * 过滤只属于指定Service的FileNode
 * 
 * @author yupeng
 *
 */
public class ServiceFileNodeFilter implements FileNodeFilter {
	private Service service;
	
	public ServiceFileNodeFilter(Service service) {
		this.service = service;
	}

	@Override
	public boolean filter(FileNode fileNode) {
		return fileNode.getServiceId().equals(service.getServiceId());
	}

}
