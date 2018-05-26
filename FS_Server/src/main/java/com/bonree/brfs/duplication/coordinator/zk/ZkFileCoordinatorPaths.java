package com.bonree.brfs.duplication.coordinator.zk;

import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.coordinator.FileNode;

public final class ZkFileCoordinatorPaths {
	//文件协调模块的根节点名
	public static final String COORDINATOR_ROOT = "fileCoordinator";
	//文件仓库节点名
	public static final String COORDINATOR_FILESTORE = "fileStore";
	//文件接收节点名
	public static final String COORDINATOR_SINK = "fileSink";
	//Leader选举路径
	public static final String COORDINATOR_LEADER = "leader";
	
	//保存Sink节点的容器路径
	public static String sinkContainerPath() {
		return ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK);
	}

	// 构建Service的Sink路径
	public static String buildSinkPath(Service service) {
		return ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK,
				service.getServiceId());
	}

	// 构建Sink中FileNode的路径
	public static String buildSinkFileNodePath(FileNode node) {
		return ZKPaths.makePath(COORDINATOR_ROOT, COORDINATOR_SINK,
				node.getServiceId(), node.getName());
	}
}
