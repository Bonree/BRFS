package com.br.disknode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.br.disknode.utils.PooledThreadFactory;

public class DiskNodeManager {
	private ExecutorService threadPools;
	
	private static final int DEFAULT_WORKER_NUMBER = 5;
	private int workerCapacity = DEFAULT_WORKER_NUMBER;
	private List<WriteWorker> workerList = new ArrayList<WriteWorker>(workerCapacity);
	private WriteWorkerSelector workerSelector;
	
	private Map<String, WriteWorker> workers = new HashMap<String, WriteWorker>();
	
	public DiskNodeManager() {
		this.threadPools = Executors.newFixedThreadPool(workerCapacity, new PooledThreadFactory("write_worker"));
		this.workerSelector = new RandomWriteWorkerSelector();
	}
	
	public void start() {
		for(int i = 0; i < workerCapacity; i++) {
			WriteWorker worker = new WriteWorker();
			threadPools.submit(worker);
			workerList.add(worker);
		}
	}
	
	public void stop() {
		for(WriteWorker worker : workerList) {
			worker.quit();
		}
		
		threadPools.shutdownNow();
	}
	
	public void createWriter(String path, boolean override) throws IOException {
		WriteWorker worker = workers.get(path);
		
		if(worker == null) {
			synchronized (workers) {
				if(!workers.containsKey(path)) {
					worker = workerSelector.select(workerList);
					worker.createWriter(path, override);
					workers.put(path, worker);
				}
			}
		}
	}
	
	public void put(String path, byte[] bytes) throws IOException, InterruptedException {
		WriteWorker worker = workers.get(path);
		
		if(worker == null) {
			throw new IOException("no available DiskWriter!");
		}
		
		worker.put(new WriteItem(path, bytes));
	}
	
	public void close(String path) throws IOException {
		WriteWorker worker = workers.get(path);
		if(worker != null) {
			worker.close(path);
			workers.remove(path);
		}
	}
}
