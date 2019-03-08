package com.bonree.brfs.duplication.filenode.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeFilter;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;

public class ZkFileNodeStorer implements FileNodeStorer {
	private static final Logger LOG = LoggerFactory.getLogger(ZkFileNodeStorer.class);
	
	private CuratorFramework client;
	private String storePath;

	public ZkFileNodeStorer(CuratorFramework client, String nodePath) throws Exception {
		this.client = client;
		this.storePath = ZKPaths.makePath(ZkFileCoordinatorPaths.COORDINATOR_ROOT, nodePath);
		this.client.createContainers(storePath);
	}

	@Override
	public void save(FileNode fileNode) {
		String fileNodePath = ZKPaths.makePath(storePath, fileNode.getName());

		try {
			client.create().creatingParentsIfNeeded().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(String fileName) {
		String fileNodePath = ZKPaths.makePath(storePath, fileName);
		
		try {
			client.delete().quietly().forPath(fileNodePath);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public FileNode getFileNode(String fileName) {
		String fileNodePath = ZKPaths.makePath(storePath, fileName);
		
		try {
			byte[] bytes = client.getData().forPath(fileNodePath);
			return JsonUtils.toObject(bytes, FileNode.class);
		} catch(NoNodeException e) {
			LOG.warn("no node is find in zk storer for file[{}]", fileName);
			return null;
		} catch (Exception e) {
			LOG.error("get file node[{}] error", fileName, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void update(FileNode fileNode) {
		String fileNodePath = ZKPaths.makePath(storePath, fileNode.getName());
		
		try {
			client.setData().forPath(fileNodePath, JsonUtils.toJsonBytes(fileNode));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public List<FileNode> listFileNodes() {
		return listFileNodes(null);
	}

	@Override
	public List<FileNode> listFileNodes(FileNodeFilter filter) {
		List<FileNode> fileNodes = new ArrayList<FileNode>();
		for(String fileName : getFileNameList()) {
			FileNode node = getFileNode(fileName);
			if(node == null) {
				continue;
			}
			
			if(filter != null && !filter.filter(node)) {
				continue;
			}
			
			fileNodes.add(node);
		}
		
		return fileNodes;
	}

	private List<String> getFileNameList() {
		try {
			return client.getChildren().forPath(storePath);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int fileNodeSize() {
		try {
			return client.getChildren().forPath(storePath).size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
