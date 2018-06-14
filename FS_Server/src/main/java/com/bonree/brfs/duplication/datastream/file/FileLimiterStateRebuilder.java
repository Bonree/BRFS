package com.bonree.brfs.duplication.datastream.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.server.identification.ServerIDManager;

public class FileLimiterStateRebuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FileLimiterStateRebuilder.class);
	
	private DiskNodeConnectionPool connectionPool;
	private ServerIDManager idManager;
	
	public FileLimiterStateRebuilder(DiskNodeConnectionPool connectionPool, ServerIDManager idManager) {
		this.connectionPool = connectionPool;
		this.idManager = idManager;
	}
	
	private int[] getMetaInfo(FileNode fileNode) {
		int[] metaInfo = null;
		DuplicateNode[] nodes = fileNode.getDuplicateNodes();
		for(DuplicateNode node : nodes) {
			DiskNodeConnection connection = connectionPool.getConnection(node);
			
			LOG.info("connection ==" + connection);
			if(connection == null || connection.getClient() == null) {
				continue;
			}
			
			String serverId = idManager.getOtherSecondID(node.getId(), fileNode.getStorageId());
			String filePath = FilePathBuilder.buildFilePath(fileNode.getStorageName(), serverId, fileNode.getCreateTime(), fileNode.getName());
			metaInfo = connection.getClient().getWritingFileMetaInfo(filePath);
			
			if(metaInfo != null) {
				break;
			}
		}
		
		return metaInfo;
	}
	
	public FileLimiter rebuild(FileNode fileNode) {
		int[] metaInfo = getMetaInfo(fileNode);
		if(metaInfo == null) {
			LOG.error("Can not get Metadata of fileNode[{}]", fileNode.getName());
			return null;
		}
		
		LOG.info("rebuild fileNode[{}] with length[{}], sequence[{}]", fileNode.getName(), metaInfo[1], metaInfo[0] + 1);
		FileLimiter file = new FileLimiter(fileNode, DuplicationEnvironment.DEFAULT_MAX_FILE_SIZE, metaInfo[1]/*文件大小*/, metaInfo[0] + 1/*文件序列号*/);
		
		return file;
	}
	
	public FileLimiter rebuild(FileLimiter fileLimiter) {
		FileNode fileNode = fileLimiter.getFileNode();
		int[] metaInfo = getMetaInfo(fileNode);
		if(metaInfo == null) {
			LOG.error("Can not get Metadata of fileLimiter[{}]", fileNode.getName());
			return null;
		}
		
		LOG.info("rebuild fileLimiter[{}] with length[{}], sequence[{}]", fileNode.getName(), metaInfo[1], metaInfo[0] + 1);
		fileLimiter.setLength(metaInfo[1]);
		fileLimiter.updateSequence(metaInfo[0] + 1);
		
		return fileLimiter;
	}
}
