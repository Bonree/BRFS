package com.bonree.brfs.duplication.coordinator.zk;

public final class ZkFileCoordinatorPaths {
	//文件协调模块的根节点名
	public static final String COORDINATOR_ROOT = "fileCoordinator";
	//文件仓库节点名
	public static final String COORDINATOR_FILESTORE = "fileStore";
	//文件接收节点名
	public static final String COORDINATOR_SINK = "fileSink";
	//Leader选举路径
	public static final String COORDINATOR_LEADER = "leader";
}
