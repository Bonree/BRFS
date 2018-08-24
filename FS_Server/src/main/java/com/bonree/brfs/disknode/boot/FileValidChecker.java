package com.bonree.brfs.disknode.boot;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.filenode.zk.ZkFileCoordinatorPaths;

public class FileValidChecker {
	private static final Logger LOG = LoggerFactory.getLogger(FileValidChecker.class);
	
	private CuratorFramework client;
	
	public FileValidChecker() throws InterruptedException {
		String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.newClient(zkAddresses, 5 * 1000, 30 * 1000, retryPolicy);
		client.start();
		client.blockUntilConnected();
		
		client.usingNamespace("brfs/" + Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME));
	}
	
	public boolean isValid(String fileName) {
		try {
			Stat stat = client.usingNamespace("brfs/" + Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME))
			.checkExists().forPath(ZKPaths.makePath(ZkFileCoordinatorPaths.COORDINATOR_ROOT, ZkFileCoordinatorPaths.COORDINATOR_FILESTORE, fileName));
			
			return stat != null;
		} catch (Exception e) {
			LOG.error("check exist for [{}]", fileName, e);
		}
		
		return false;
	}
}
