package com.bonree.brfs.duplication.datastream.file;

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
import com.bonree.brfs.duplication.storagename.StorageNameNode;
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
	public FileObject createFile(StorageNameNode storageRegion) {
		DuplicateNode[] nodes = duplicationNodeSelector.getDuplicationNodes(storageRegion.getId(), storageRegion.getReplicateCount());
		if(nodes.length == 0) {
			LOG.error("No available duplication node to build FileNode");
			//没有磁盘节点可用
			return null;
		}
		
		FileNode fileNode = new FileNode();
		fileNode.setStorageName(storageRegion.getName());
		fileNode.setStorageId(storageRegion.getId());
		fileNode.setServiceId(service.getServiceId());
		fileNode.setServiceGroup(service.getServiceGroup());
		fileNode.setName(FileNameBuilder.createFile(idManager, storageRegion, nodes));
		fileNode.setDuplicateNodes(nodes);
		fileNode.setTimeDuration(storageRegion.getPartitionDuration());
		
		long capacity = -1;
		for(DuplicateNode node : nodes) {
			LOG.info("start init node[{}] for file[{}]", node, fileNode.getName());
			DiskNodeConnection connection = connectionPool.getConnection(node);
			if(connection == null || connection.getClient() == null) {
				LOG.info("can not write header for file[{}] because [{}] is disconnected", fileNode.getName(), node);
				continue;
			}
			
			String serverId = idManager.getOtherSecondID(node.getId(), storageRegion.getId());
			String filePath = FilePathBuilder.buildFilePath(fileNode, serverId);
			
			long result = connection.getClient().openFile(filePath, storageRegion.getFileCapacity());
			if(result < 0) {
				continue;
			}
			
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
			LOG.error("can not open file at any duplicate node for file[{}]", fileNode.getName());
			return null;
		}
		
		try {
			fileNode.setCapacity(capacity);
			fileNodeStorer.save(fileNode);
			
			return new FileObject(fileNode);
		} catch (Exception e) {
			LOG.error("store file node[{}] error!", fileNode.getName(), e);
		}
		
		return null;
	}

}
