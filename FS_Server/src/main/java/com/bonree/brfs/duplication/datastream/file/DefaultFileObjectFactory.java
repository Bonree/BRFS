package com.bonree.brfs.duplication.datastream.file;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNameBuilder;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.FilePathBuilder;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileObjectFactory implements FileObjectFactory {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileObjectFactory.class);
	
	private Service service;
	private FileNodeStorer fileNodeStorer;
	private DuplicateNodeSelector duplicationNodeSelector;
	private ServerIDManager idManager;
	private DiskNodeConnectionPool connectionPool;
	
	public DefaultFileObjectFactory(Service service,
			FileNodeStorer fileNodeStorer,
			DuplicateNodeSelector duplicationNodeSelector,
			ServerIDManager serverIDManager,
			DiskNodeConnectionPool connectionPool) {
		this.service = service;
		this.fileNodeStorer = fileNodeStorer;
		this.duplicationNodeSelector = duplicationNodeSelector;
		this.idManager = serverIDManager;
		this.connectionPool = connectionPool;
	}

	@Override
	public FileObject createFile(StorageRegion storageRegion) {
		DuplicateNode[] nodes = duplicationNodeSelector.getDuplicationNodes(storageRegion.getId(), storageRegion.getReplicateNum());
		if(nodes.length == 0) {
			LOG.error("No available duplication node to build FileNode");
			//没有磁盘节点可用
			return null;
		}
		
		FileNode.Builder fileNodeBuilder = FileNode.newBuilder()
				.setStorageName(storageRegion.getName())
				.setStorageId(storageRegion.getId())
				.setServiceId(service.getServiceId())
				.setServiceGroup(service.getServiceGroup())
				.setName(FileNameBuilder.createFile(idManager, storageRegion, nodes))
				.setDuplicateNodes(nodes)
				.setTimeDuration(Duration.parse(storageRegion.getFilePartitionDuration()).toMillis());
		
		long capacity = -1;
		for(DuplicateNode node : nodes) {
			LOG.info("start init node[{}] for file[{}]", node, fileNodeBuilder.build().getName());
			DiskNodeConnection connection = connectionPool.getConnection(node.getGroup(), node.getId());
			if(connection == null || connection.getClient() == null) {
				LOG.info("can not write header for file[{}] because [{}] is disconnected", fileNodeBuilder.build().getName(), node);
				continue;
			}
			
			String serverId = idManager.getOtherSecondID(node.getId(), storageRegion.getId());
			String filePath = FilePathBuilder.buildFilePath(fileNodeBuilder.build(), serverId);
			
			LOG.info("client opening file [{}]", filePath);
			long result = connection.getClient().openFile(filePath, storageRegion.getFileCapacity());
			if(result < 0) {
				continue;
			}
			
			LOG.info("open file[{}] from datanode[{}] with capacity[{}]", filePath, node, result);
			if(capacity < 0) {
				capacity = result;
				continue;
			}
			
			if(capacity != result) {
				LOG.error("different capacity be received from different dupcate nodes");
				capacity = -1;
				break;
			}
		}
		
		//如果没有一个磁盘节点写入头数据成功，则放弃使用此文件节点
		if(capacity < 0) {
			LOG.error("can not open file at any duplicate node for file[{}]", fileNodeBuilder.build().getName());
			return null;
		}
		
		FileNode fileNode = fileNodeBuilder.setCapacity(capacity).build();
		try {
			fileNodeStorer.save(fileNode);
			
			return new FileObject(fileNode);
		} catch (Exception e) {
			LOG.error("store file node[{}] error!", fileNode.getName(), e);
		}
		
		return null;
	}

}
