package com.bonree.brfs.duplication.datastream.file;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
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
	
	private FileSynchronizer fileRecovery;
	private DiskNodeConnectionPool connectionPool;
	private FileCoordinator fileCoordinator;
	private ServerIDManager idManager;
	private ServiceManager serviceManager;
	
	public FileLimiterCloser(FileSynchronizer fileRecovery,
			DiskNodeConnectionPool connectionPool,
			FileCoordinator fileCoordinator,
			ServiceManager serviceManager,
			ServerIDManager idManager) {
		this.fileRecovery = fileRecovery;
		this.connectionPool = connectionPool;
		this.fileCoordinator = fileCoordinator;
		this.serviceManager = serviceManager;
		this.idManager = idManager;
	}
	
	@Override
	public void close(FileLimiter file) throws Exception {
		fileRecovery.synchronize(file.getFileNode(), new FileCloseConditionChecker(file));
	}
	
	private class FileCloseConditionChecker implements FileSynchronizeCallback {
		private FileLimiter file;
		
		public FileCloseConditionChecker(FileLimiter file) {
			this.file = file;
		}

		@Override
		public void complete(FileNode fileNode) {
			LOG.info("start to close file node[{}]", fileNode.getName());
			for(DuplicateNode node : fileNode.getDuplicateNodes()) {
				if(node.getGroup().equals(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP)) {
					LOG.info("Ignore virtual duplicate node[{}]", node);
					continue;
				}
				
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.info("close error because node[{}] is disconnected!", node);
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				String serverId = idManager.getOtherSecondID(node.getId(), fileNode.getStorageId());
				String filePath = FilePathBuilder.buildFilePath(fileNode.getStorageName(), serverId, fileNode.getCreateTime(), fileNode.getName());
				
				try {
					LOG.info("file tailer--{}", file.getTailer().length);
					WriteResult result = client.writeData(filePath, -2, file.getTailer());
					LOG.info("write file[{}] TAILER result[{}]", filePath, result);
					if(result != null) {
						client.closeFile(filePath);
						fileCoordinator.delete(file.getFileNode());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void error(Throwable cause) {
			cause.printStackTrace();
			try {
				//对于没办法处理的文件，只能放弃了
				fileCoordinator.delete(file.getFileNode());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}
