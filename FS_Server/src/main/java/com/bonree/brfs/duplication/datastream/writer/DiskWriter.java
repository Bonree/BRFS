package com.bonree.brfs.duplication.datastream.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataObject;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class DiskWriter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(DiskWriter.class);
	
	private ExecutorService writeWorkers;
	private DiskNodeConnectionPool connectionPool;
	
	private FilePathMaker pathMaker;
	
	public DiskWriter(int workerNum, DiskNodeConnectionPool connectionPool, FilePathMaker pathMaker) {
		this.writeWorkers = Executors.newFixedThreadPool(workerNum, new PooledThreadFactory("disk_write_worker"));
		this.connectionPool = connectionPool;
		this.pathMaker = pathMaker;
	}
	
	public void write(FileObject file, List<DataObject> datas, WriteProgressListener listener) {
		DuplicateNode[] nodes = file.node().getDuplicateNodes();
		
		DiskWriterCallback writerCallback = new DiskWriterCallback(nodes.length, datas, listener);
		for(int i = 0; i < nodes.length; i++) {
			writeWorkers.submit(new DiskWriteTask(file, datas, nodes[i], i, writerCallback));
		}
	}
	
	@Override
	public void close() {
		writeWorkers.shutdown();
	}
	
	public static interface WriteProgressListener {
		void writeCompleted(FileObject file, boolean errorOccurred);
	}
	
	private class DiskWriteTask implements Runnable {
		private FileObject file;
		private List<DataObject> datas;
		private DiskWriterCallback callback;
		private DuplicateNode node;
		private final int index;
		
		public DiskWriteTask(FileObject file, List<DataObject> datas, DuplicateNode node, int index, DiskWriterCallback callback) {
			this.file = file;
			this.datas = datas;
			this.node = node;
			this.index = index;
			this.callback = callback;
		}

		@Override
		public void run() {
			DataOut[] dataOuts = new DataOut[datas.size()];
			
			try {
				DiskNodeConnection conn = connectionPool.getConnection(node.getGroup(), node.getId());
				if(conn == null || conn.getClient() == null) {
					return;
				}
				
				WriteData[] writeDatas = new WriteData[datas.size()];
				for(int i = 0; i < writeDatas.length; i++) {
					writeDatas[i] = new WriteData();
					writeDatas[i].setBytes(datas.get(i).getBytes());
				}
				
				WriteResult[] results = null;
				try {
					results = conn.getClient().writeDatas(pathMaker.buildPath(file.node(), node), writeDatas);
				} catch (IOException e) {
					LOG.error("write file[{}] to disk error!", file.node().getName());
				}
				
				if(results != null) {
					for(int i = 0; i < dataOuts.length; i++) {
						if(results[i] == null) {
							break;
						}
						
						final WriteResult result = results[i];
						dataOuts[i] = new DataOut() {
							
							@Override
							public long offset() {
								return result.getOffset();
							}
							
							@Override
							public int length() {
								return result.getSize();
							}
						};
					}
				}
			} finally {
				callback.complete(file, index, dataOuts);
			}
		}
		
	}
}
