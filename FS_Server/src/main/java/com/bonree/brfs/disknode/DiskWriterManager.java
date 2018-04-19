package com.bonree.brfs.disknode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.server.handler.WriteData;

public class DiskWriterManager implements LifeCycle {
	private ExecutorService threadPool;
	
	//默认的写Worker线程数量
	private static final int DEFAULT_WORKER_NUMBER = 5;
	private WriteWorkerGroup workerGroup;
	private WriteWorkerSelector workerSelector;
	
	private Map<String, DiskWriter> runningWriters = new HashMap<String, DiskWriter>();
	
	public DiskWriterManager() {
		this(DEFAULT_WORKER_NUMBER);
	}
	
	public DiskWriterManager(int workerNum) {
		this(workerNum, new RandomWriteWorkerSelector());
	}
	
	public DiskWriterManager(int workerNum, WriteWorkerSelector selector) {
		this.workerGroup = new WriteWorkerGroup(workerNum);
		this.threadPool = new ThreadPoolExecutor(workerNum,
				workerNum,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("write_worker"));
		this.workerSelector = selector;
		this.workerGroup.fill(() -> new WriteWorker());
	}
	
	@Override
	public void start() {
		workerGroup.forEach((WriteWorker worker) -> threadPool.submit(worker));
	}
	
	@Override
	public void stop() {
		workerGroup.forEach((WriteWorker worker) -> worker.quit());
		
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
	
	public void writeAsync(String path, WriteData item, InputEventCallback callback) {
		DiskWriter writer = runningWriters.get(path);
		
		if(writer == null) {
			callback.error(new IOException("no available DiskWriter!"));
			return;
		}
		
		InputEvent event = new InputEvent(writer, item);
		event.setInputEventCallback(callback);
		writer.worker().put(event);
	}
	
	public boolean isWriting(String path) {
		return runningWriters.containsKey(path);
	}
	
	public void close(String path) {
		DiskWriter writer = runningWriters.remove(path);
		CloseUtils.closeQuietly(writer);
	}
}
