package com.bonree.brfs.duplication.datastream.file;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileCoordinator;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.synchronize.FileSynchronizeCallback;
import com.bonree.brfs.duplication.synchronize.FileSynchronizer;
import com.bonree.brfs.server.identification.ServerIDManager;

public class FileLimiterCloser implements FileCloseListener {
	private static final Logger LOG = LoggerFactory.getLogger(FileLimiterCloser.class);
	
	private FileSynchronizer fileSynchronizer;
	private DiskNodeConnectionPool connectionPool;
	private FileCoordinator fileCoordinator;
	private ServerIDManager idManager;
	
	private static final long DEFAULT_FILE_CLOSE_RETRY_INTERVAL_MILLIS = 10 * 1000;
	
	public FileLimiterCloser(FileSynchronizer fileRecovery,
			DiskNodeConnectionPool connectionPool,
			FileCoordinator fileCoordinator,
			ServerIDManager idManager) {
		this.fileSynchronizer = fileRecovery;
		this.connectionPool = connectionPool;
		this.fileCoordinator = fileCoordinator;
		this.idManager = idManager;
	}
	
	@Override
	public void close(FileLimiter file) throws Exception {
		fileSynchronizer.synchronize(file.getFileNode(), new FileCloseConditionChecker(file.getFileNode()));
	}
	
	public void closeFileNode(FileNode fileNode) {
		LOG.info("start to close file node[{}]", fileNode.getName());
		boolean closeCompleted = true;
		for(DuplicateNode node : fileNode.getDuplicateNodes()) {
			if(node.getGroup().equals(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP)) {
				LOG.info("Ignore virtual duplicate node[{}]", node);
				continue;
			}
			
			DiskNodeConnection connection = connectionPool.getConnection(node);
			if(connection == null || connection.getClient() == null) {
				LOG.info("close error because node[{}] is disconnected!", node);
				closeCompleted = false;
				continue;
			}
			
			DiskNodeClient client = connection.getClient();
			String serverId = idManager.getOtherSecondID(node.getId(), fileNode.getStorageId());
			String filePath = FilePathBuilder.buildFilePath(fileNode.getStorageName(), serverId, fileNode.getCreateTime(), fileNode.getName());
			
			LOG.info("closing file[{}]", filePath);
			boolean closed = client.closeFile(filePath);
			LOG.info("close file[{}] result->{}", filePath, closed);
			
			closeCompleted &= closed;
		}
		
		if(closeCompleted) {
			try {
				fileCoordinator.delete(fileNode);
			} catch (Exception e) {
				LOG.error("delete file[{}] from file coordinator failed", fileNode.getName());
			}
		} else {
			fileSynchronizer.synchronize(fileNode, new FileCloseConditionChecker(fileNode), DEFAULT_FILE_CLOSE_RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
		}
	}
	
	private class FileCloseConditionChecker implements FileSynchronizeCallback {
		private FileNode file;
		
		public FileCloseConditionChecker(FileNode file) {
			this.file = file;
		}

		@Override
		public void complete(FileNode fileNode) {
			LOG.info("close file[{}] after sync", fileNode.getName());
			closeFileNode(fileNode);
		}

		@Override
		public void error(Throwable cause) {
			LOG.error("sync file to close error", cause);
			try {
				//对于没办法处理的文件，只能放弃了
				fileCoordinator.delete(file);
			} catch (Exception e) {
				LOG.error("delete file node error", e);
			}
		}
		
	}
}
