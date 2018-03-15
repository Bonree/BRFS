package com.br.disknode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.br.disknode.watch.WatchMarket;
import com.br.disknode.watch.Watcher;

public class WriteWorker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(WriteWorker.class);
	
	private Map<String, DiskWriter> writers = new HashMap<String, DiskWriter>();
	
	private static final int QUEUE_CAPACITY = 100;
	private ArrayBlockingQueue<WriteItem> itemQueue = new ArrayBlockingQueue<WriteItem>(QUEUE_CAPACITY);
	
	private volatile boolean isQuit = false;
	
	public static final String WATCHER_NAME = "WriteWorker"; 
	
	public static final String WORKER_QUEUE = "worker_queue";
	public static final String WORKER_WRITER = "worker_writer";
	
	public void createWriter(String path) throws IOException {
		createWriter(path, true);
	}
	
	public void createWriter(String path, boolean override) throws IOException {
		writers.put(path, new DiskWriter(path, override));
		LOG.info("create Writer for [{}], current size[{}]", path, writers.size());
	}
	
	public void close(String path) throws IOException {
		DiskWriter writer = writers.get(path);
		if(writer != null) {
			writer.close();
			writers.remove(path);
		}
		
		LOG.info("Close Writer for[{}]", path);
	}
	
	public void put(WriteItem item) throws InterruptedException {
		itemQueue.put(item);
	}
	
	public void quit() {
		isQuit = true;
	}

	@Override
	public void run() {
		WriteWorkerWatcher writeWorkerWatcher = new WriteWorkerWatcher();
		try {
			WatchMarket.get().addWatcher(WATCHER_NAME, writeWorkerWatcher);
			while(!isQuit) {
				try {
					WriteItem item = itemQueue.take();
					
					DiskWriter writer = writers.get(item.getFilePath());
					
					try {
						writer.beginWriting();
						//write datas
						writer.write(item.getData());
						
						WriteInfo writeInfo = writer.endWriting();
						//TODO handle writeInfo
					} catch (IOException e) {
						LOG.error("back writing", e);
						writer.backWriting();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} finally {
			WatchMarket.get().removeWatcher(WATCHER_NAME, writeWorkerWatcher);
		}
	}
	
	private class WriteWorkerWatcher implements Watcher<Map<String, Integer>> {

		@Override
		public Map<String, Integer> watch() {
			Map<String, Integer> metrics = new HashMap<String, Integer>();
			metrics.put(WORKER_QUEUE, itemQueue.size());
			metrics.put(WORKER_WRITER, writers.size());
			
			return metrics;
		}
		
	}
}
