package com.bonree.brfs.duplication.datastream.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizeCallback;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizer;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class DefaultFileObjectCloser implements FileObjectCloser, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileObjectCloser.class);
	
	private ExecutorService closeThreads;
	
	private FileObjectSynchronizer fileSynchronizer;
	private DiskNodeConnectionPool connectionPool;
	private FileNodeStorer fileNodeStorer;
	
	private FilePathMaker pathMaker;
	
	public DefaultFileObjectCloser(int threadNum,
			FileObjectSynchronizer fileSynchronizer,
			FileNodeStorer fileNodeStorer,
			DiskNodeConnectionPool connectionPool,
			FilePathMaker pathMaker) {
		this.closeThreads = Executors.newFixedThreadPool(threadNum, new PooledThreadFactory("file_closer"));
		this.fileSynchronizer = fileSynchronizer;
		this.fileNodeStorer = fileNodeStorer;
		this.connectionPool = connectionPool;
		this.pathMaker = pathMaker;
	}
	
	@Override
	public void close() throws IOException {
		closeThreads.shutdown();
	}

	@Override
	public void close(FileObject file, boolean syncIfFailed) {
		closeThreads.submit(new CloseProcessor(file, syncIfFailed));
	}

	private class CloseProcessor implements Runnable {
		private FileObject file;
		private boolean syncIfNeeded;
		
		public CloseProcessor(FileObject file, boolean syncIfNeeded) {
			this.file = file;
			this.syncIfNeeded = syncIfNeeded;
		}
		
		private boolean closeDiskNodes() {
			boolean closeAll = true;
			FileNode fileNode = file.node();
			
			long closeCode = -1;
			for(DuplicateNode node : fileNode.getDuplicateNodes()) {
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.info("close error because node[{}] is disconnected!", node);
					closeAll = false;
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				String filePath = pathMaker.buildPath(fileNode, node);
				
				LOG.info("closing file[{}]", filePath);
				long code = client.closeFile(filePath);
				if(code < 0) {
					closeAll = false;
					continue;
				}
				
				if(closeCode == -1) {
					closeCode = code;
					continue;
				}
				
				if(closeCode != code) {
					closeAll = false;
				}
			}
			
			return closeAll;
		}

		@Override
		public void run() {
			if(!closeDiskNodes() && syncIfNeeded) {
				fileSynchronizer.synchronize(file, new FileObjectSynchronizeCallback() {
					
					@Override
					public void complete(FileObject file, long fileLength) {
						LOG.info("final length is [{}] before close file[{}]", fileLength, file.node().getName());
						close(file, true);
					}

					@Override
					public void timeout(FileObject file) {
						close(file, false);
					}
				});
				
				return;
			}
			
			try {
				fileNodeStorer.delete(file.node().getName());
			} catch (Exception e) {
				LOG.error("delete file[{}] from file coordinator failed", file.node().getName());
			}
		}
		
	}
}
