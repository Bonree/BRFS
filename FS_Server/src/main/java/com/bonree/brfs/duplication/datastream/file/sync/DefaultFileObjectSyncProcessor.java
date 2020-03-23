package com.bonree.brfs.duplication.datastream.file.sync;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.filesync.FileObjectSyncState;
import com.bonree.brfs.common.filesync.SyncStateCodec;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class DefaultFileObjectSyncProcessor implements FileObjectSyncProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileObjectSyncProcessor.class);
	
	private DiskNodeConnectionPool connectionPool;
	private FilePathMaker pathMaker;
	
	@Inject
	public DefaultFileObjectSyncProcessor(DiskNodeConnectionPool connectionPool, FilePathMaker pathMaker) {
		this.connectionPool = connectionPool;
		this.pathMaker = pathMaker;
	}

	@Override
	public boolean process(FileObjectSyncTask task) {
		FileObject file = task.file();
		
		LOG.info("start to synchronize file[{}]", file.node().getName());
		DuplicateNode[] nodeList = file.node().getDuplicateNodes();
		
		boolean syncAccomplished = true;
		List<FileObjectSyncState> fileStateList = getFileStateList(file.node());
		
		if(fileStateList.isEmpty()) {
			//文件所在的所有磁盘节点都处于异常状态
			LOG.error("No available duplicate node is found to sync file[{}]", file.node().getName());
			
			if(task.isExpired()) {
				task.callback().timeout(file);
				return true;
			}
			
			return false;
		}
		
		if(fileStateList.size() != nodeList.length) {
			//文件所在的所有磁盘节点中有部分不可用，这种情况先同步可用的磁盘节点信息
			LOG.warn("Not all duplicate nodes are available to sync file[{}]", file.node().getName());
			syncAccomplished = false;
		}
		
		long maxLength = -1;
		for(FileObjectSyncState state : fileStateList) {
			maxLength = Math.max(maxLength, state.getFileLength());
		}
		
		List<FileObjectSyncState> lack = new ArrayList<FileObjectSyncState>();
		List<FileObjectSyncState> full = new ArrayList<FileObjectSyncState>();
		for(FileObjectSyncState state : fileStateList) {
			if(state.getFileLength() != maxLength) {
				lack.add(state);
			} else {
				full.add(state);
			}
		}
		
		if(lack.isEmpty()) {
			if(syncAccomplished) {
				LOG.info("file[{}] is ok!", file.node().getName());
				task.callback().complete(file, maxLength);
				return true;
			} else {
				LOG.info("file[{}] is lack of some duplicate node!", file.node().getName());
				if(task.isExpired()) {
					LOG.info("file[{}] sync is expired!", file.node().getName());
					task.callback().timeout(file);
					return true;
				}
				
				return false;
			}
		} else {
			syncAccomplished &= doSynchronize(file.node(), maxLength, lack, full);
			if(syncAccomplished) {
				LOG.info("file[{}] sync is completed!", file.node().getName());
				task.callback().complete(file, maxLength);
				return true;
			} else {
				LOG.info("file[{}] sync is failed!", file.node().getName());
				if(task.isExpired()) {
					LOG.info("file[{}] sync is expired!", file.node().getName());
					task.callback().timeout(file);
					return true;
				}
				
				return false;
			}
		}
	}

	private List<FileObjectSyncState> getFileStateList(FileNode fileNode) {
		List<FileObjectSyncState> fileStateList = new ArrayList<FileObjectSyncState>();
		
		for(DuplicateNode node : fileNode.getDuplicateNodes()) {
			DiskNodeConnection connection = connectionPool.getConnection(node.getGroup(), node.getId());
			if(connection == null || connection.getClient() == null) {
				LOG.error("duplication node[{}, {}] of [{}] is not available, that's maybe a trouble!", node.getGroup(), node.getId(), fileNode.getName());
				continue;
			}
			
			String filePath = pathMaker.buildPath(fileNode, node);
			LOG.info("checking---{}", filePath);
			long fileLength = connection.getClient().getFileLength(filePath);
			
			if(fileLength < 0) {
				LOG.error("duplication node[{}, {}] of [{}] can not get file length, that's maybe a trouble!", node.getGroup(), node.getId(), fileNode.getName());
				continue;
			}
			
			LOG.info("server{} -- {}", node.getId(), fileLength);
			
			fileStateList.add(new FileObjectSyncState(node.getGroup(), node.getId(), filePath, fileLength));
		}
		
		return fileStateList;
	}
	
	private boolean doSynchronize(FileNode fileNode, long correctLength, List<FileObjectSyncState> lacks, List<FileObjectSyncState> fulls) {
		List<String> fullStates = new ArrayList<String>();
		for(FileObjectSyncState state : fulls) {
			fullStates.add(SyncStateCodec.toString(state));
		}
		
		boolean allSynced = true;
		for(FileObjectSyncState state : lacks) {
			DiskNodeConnection connection = connectionPool.getConnection(state.getServiceGroup(), state.getServiceId());
			if(connection == null) {
				LOG.error("can not recover file[{}], because of lack of connection to service[{}, {}]",
						fileNode.getName(), state.getServiceGroup(), state.getServiceId());
				allSynced = false;
				continue;
			}
			
			DiskNodeClient client = connection.getClient();
			if(client == null) {
				allSynced = false;
				continue;
			}
			
			LOG.info("start synchronize file[{}] at data node[{}, {}]", fileNode.getName(), state.getServiceGroup(), state.getServiceId());
			if(!client.recover(state.getFilePath(), state.getFileLength(), fullStates)) {
				LOG.error("can not synchronize file[{}] at data node[{}, {}]", fileNode.getName(), state.getServiceGroup(), state.getServiceId());
				allSynced = false;
			}
			
		}
		
		return allSynced;
	}
}
