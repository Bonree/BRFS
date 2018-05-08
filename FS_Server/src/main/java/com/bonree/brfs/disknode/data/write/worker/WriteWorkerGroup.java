package com.bonree.brfs.disknode.data.write.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.bonree.brfs.common.utils.LifeCycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;

public class WriteWorkerGroup implements LifeCycle {
	
	private ExecutorService workerThreads;
	
	private List<WriteWorker> workerList = new ArrayList<WriteWorker>();
	
	public WriteWorkerGroup(int threadNum) {
		this.workerThreads = new ThreadPoolExecutor(threadNum,
				threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(threadNum),
                new PooledThreadFactory("write_worker"));
		
		for(int i = 0; i < threadNum; i++) {
			workerList.add(new WriteWorker());
		}
	}
	
	public List<WriteWorker> getWorkerList() {
		return workerList;
	}
	
	@Override
	public void start() {
		workerList.forEach(new Consumer<WriteWorker>() {

			@Override
			public void accept(WriteWorker worker) {
				workerThreads.submit(worker);
			}
			
		});
	}

	@Override
	public void stop() {
		workerList.forEach(new Consumer<WriteWorker>() {

			@Override
			public void accept(WriteWorker worker) {
				worker.quit();
			}
			
		});
		
		workerThreads.shutdown();
	}
}
