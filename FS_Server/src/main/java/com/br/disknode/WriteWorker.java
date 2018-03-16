package com.br.disknode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.br.disknode.watch.WatchMarket;
import com.br.disknode.watch.Watcher;

public class WriteWorker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(WriteWorker.class);
	
	private static final int QUEUE_CAPACITY = 100;
	private ArrayBlockingQueue<InputEvent> itemQueue = new ArrayBlockingQueue<InputEvent>(QUEUE_CAPACITY);
	
	private volatile boolean isQuit = false;
	
	public static final String WATCHER_NAME = "WriteWorker"; 
	
	public static final String WORKER_QUEUE = "worker_queue";
	public static final String WORKER_WRITER = "worker_writer";
	
	public void put(InputEvent item) throws InterruptedException {
		itemQueue.put(item);
	}
	
	public void quit() {
		isQuit = true;
	}

	@Override
	public void run() {
//		WriteWorkerWatcher writeWorkerWatcher = new WriteWorkerWatcher();
		try {
//			WatchMarket.get().addWatcher(WATCHER_NAME, writeWorkerWatcher);
			while(!isQuit) {
				try {
					InputEvent item = itemQueue.take();
					
					LOG.debug("TAKE INPUT EVENT[{}]", item.getWriter().getFilePath());
					DiskWriter writer = item.getWriter();
					
					try {
						writer.beginWriting();
						writer.write(item.getData());
						
						InputResult inputResult = writer.endWriting();
						ForkJoinPool.commonPool().execute(() -> item.getInputEventCallback().complete(inputResult));
					} catch (IOException e) {
						LOG.error("back writing", e);
						writer.backWriting();
						ForkJoinPool.commonPool().execute(() -> item.getInputEventCallback().completeError(e));
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} finally {
//			WatchMarket.get().removeWatcher(WATCHER_NAME, writeWorkerWatcher);
		}
	}
	
	private class WriteWorkerWatcher implements Watcher<Map<String, Integer>> {

		@Override
		public Map<String, Integer> watch() {
			Map<String, Integer> metrics = new HashMap<String, Integer>();
			metrics.put(WORKER_QUEUE, itemQueue.size());
			
			return metrics;
		}
		
	}
}
