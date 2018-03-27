package com.bonree.brfs.disknode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bonree.brfs.disknode.utils.LifeCycle;
import com.bonree.brfs.disknode.utils.PooledThreadFactory;
import com.google.common.io.Closeables;

public class DiskWriterManager implements LifeCycle {
	private ExecutorService threadPool;
	
	//默认的写Worker线程数量
	private static final int DEFAULT_WORKER_NUMBER = 5;
	private WriteWorkerGroup workerGroup = new WriteWorkerGroup(DEFAULT_WORKER_NUMBER);
	private WriteWorkerSelector workerSelector;
	
	private Map<String, DiskWriter> runningWriters = new HashMap<String, DiskWriter>();
	
	public DiskWriterManager() {
		this.threadPool = new ThreadPoolExecutor(workerGroup.capacity(),
				workerGroup.capacity(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(workerGroup.capacity()),
                new PooledThreadFactory("write_worker"));
		this.workerSelector = new RandomWriteWorkerSelector();
		this.workerGroup.fill(new WriteWorkerGroup.Initializer() {
			
			@Override
			public WriteWorker init() {
				return new WriteWorker();
			}
		});
	}
	
	@Override
	public void start() {
		workerGroup.forEach(new WriteWorkerGroup.Visitor() {
			
			@Override
			public void visit(WriteWorker worker) {
				threadPool.submit(worker);
			}
		});
	}
	
	@Override
	public void stop() {
		workerGroup.forEach(new WriteWorkerGroup.Visitor() {
			
			@Override
			public void visit(WriteWorker worker) {
				worker.quit();
			}
		});
		
		threadPool.shutdownNow();
	}
	
	public void buildDiskWriter(String filePath, boolean override) throws IOException {
		if(!runningWriters.containsKey(filePath)) {
			synchronized(runningWriters) {
				if(!runningWriters.containsKey(filePath)) {
					runningWriters.put(filePath, new DiskWriter(filePath, override, workerSelector.select(workerGroup.workerArray())));
				}
			}
		}
	}
	
	public void writeAsync(String path, byte[] bytes, InputEventCallback callback) throws IOException, InterruptedException {
		DiskWriter writer = runningWriters.get(path);
		
		if(writer == null) {
			throw new IOException("no available DiskWriter!");
		}
		
		InputEvent item = new InputEvent(writer, bytes);
		item.setInputEventCallback(callback);
		writer.worker().put(item);
	}
	
	public void close(String path) throws IOException {
		DiskWriter writer = runningWriters.remove(path);
		Closeables.close(writer, false);
	}
}
