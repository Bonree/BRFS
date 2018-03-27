package com.bonree.brfs.duplication.coordinator.zk;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;

public class TestStorer {

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("122.11.47.17:2181", 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		FileNodeStorer storer = new ZkFileNodeStorer(client.usingNamespace("test"));
		
		FileNode node = new FileNode();
		node.setName("file_node_1");
		node.setServiceId("ser_1");
		node.setStorageName("sn_1");
		node.setDuplicates(new int[]{1, 2, 3});
		
		storer.save(node);
		
		FileNode node2 = storer.getFileNode("file_node_1");
		System.out.println("node2--" + node2.getName());
		
		node2.setServiceId("ser_2");
		storer.update("file_node_1", node2);
		
		List<FileNode> nodeList = storer.listFileNodes();
		for(FileNode nd : nodeList) {
			System.out.println("list--" + nd.getName() + ", " + nd.getServiceId());
			storer.delete(nd.getName());
		}
		
		synchronized (client) {
			client.wait();
		}
	}

}
