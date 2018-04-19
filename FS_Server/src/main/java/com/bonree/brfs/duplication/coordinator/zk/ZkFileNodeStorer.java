package com.bonree.brfs.duplication.coordinator.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeFilter;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;

public class ZkFileNodeStorer implements FileNodeStorer {

	private CuratorFramework client;

	public ZkFileNodeStorer(CuratorFramework client) {
		this.client = client;
	}

	@Override
	public void save(FileNode fileNode) {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILESTORE,
				fileNode.getName());

		try {
			client.create().creatingParentsIfNeeded().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(String fileName) {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILESTORE,
				fileName);
		
		try {
			client.delete().quietly().forPath(fileNodePath);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public FileNode getFileNode(String fileName) {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILESTORE,
				fileName);
		
		try {
			return JsonUtils.toObject(client.getData().forPath(fileNodePath), FileNode.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void update(FileNode fileNode) {
		String fileNodePath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILESTORE,
				fileNode.getName());
		
		try {
			client.setData().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<FileNode> listFileNodes(FileNodeFilter filter) {
		List<FileNode> fileNodes = new ArrayList<FileNode>();
		for(String fileName : getFileNameList()) {
			FileNode node = getFileNode(fileName);
			if(filter.filter(node)) {
				fileNodes.add(getFileNode(fileName));
			}
		}
		
		return fileNodes;
	}

	private List<String> getFileNameList() {
		String fileNodesPath = ZKPaths.makePath(
				ZkFileCoordinatorPaths.COORDINATOR_ROOT,
				ZkFileCoordinatorPaths.COORDINATOR_FILESTORE);
		
		try {
			return client.getChildren().forPath(fileNodesPath);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
