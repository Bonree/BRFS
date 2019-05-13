package com.bonree.brfs.duplication.datastream.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.delivery.ProducerClient;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.supervisor.WriteMetric;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.client.WriteResult;
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
				
				List<byte[]> dataList = new ArrayList<byte[]>();
				for(DataObject data : datas) {
					dataList.add(data.getBytes());
				}
				
				
				WriteMetric writeMetric = new WriteMetric();
				writeMetric.setMonitorTime(System.currentTimeMillis());
				writeMetric.setStorageName(file.node().getStorageName());
				writeMetric.setRegionNodeID(file.node().getServiceId());
				writeMetric.setDataNodeID(node.getId());
				writeMetric.setDataCount(0);
				writeMetric.setDataSize(0);
				writeMetric.setDataMaxSize(0);
				
				WriteResult[] results = null;
				TimeWatcher timeWatcher = new TimeWatcher();
				try {
					results = conn.getClient().writeDatas(pathMaker.buildPath(file.node(), node), dataList);
				} catch (IOException e) {
					LOG.error("write file[{}] to disk error!", file.node().getName());
				}
				
				writeMetric.setElapsedTime(timeWatcher.getElapsedTime());
				
				if(results != null) {
					for(int i = 0; i < dataOuts.length; i++) {
						if(results[i] == null) {
							break;
						}
						
						final WriteResult result = results[i];
						writeMetric.incrementDataCount(1);
						writeMetric.incrementDataSize(result.getSize());
						writeMetric.updateDataMaxSize(result.getSize());
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
				
				writeMetric.setAvgElapsedTime(writeMetric.getElapsedTime() / writeMetric.getDataCount());
				ProducerClient.getInstance().sendWriterMetric(writeMetric.toMap());
			} finally {
				callback.complete(file, index, dataOuts);
			}
		}
		
	}
}
