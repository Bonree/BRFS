package com.bonree.brfs.duplication.coordinator.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;

import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.utils.JsonUtils;

public class ZkFileNodeStorer implements FileNodeStorer {

	private CuratorFramework client;

	public ZkFileNodeStorer(CuratorFramework client) {
		this.client = client;
	}

	@Override
	public void save(FileNode fileNode) throws Exception {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILENODES,
				fileNode.getName());

		client.create().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
	}

	@Override
	public void delete(String fileName) throws Exception {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILENODES,
				fileName);
		
		client.delete().forPath(fileNodePath);
	}

	@Override
	public FileNode getFileNode(String fileName) throws Exception {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILENODES,
				fileName);
		
		return JsonUtils.toObject(client.getData().forPath(fileNodePath), FileNode.class);
	}

	@Override
	public void update(String fileName, FileNode fileNode) throws Exception {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILENODES,
				fileName);
		
		client.setData().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
	}

	@Override
	public List<FileNode> listFileNodes() throws Exception {
		List<FileNode> fileNodes = new ArrayList<FileNode>();
		for(String fileName : getFileNameList()) {
			fileNodes.add(getFileNode(fileName));
		}
		
		return fileNodes;
	}

	private List<String> getFileNameList() throws Exception {
		String fileNodesPath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILENODES);
		
		return client.getChildren().forPath(fileNodesPath);
	}
}
