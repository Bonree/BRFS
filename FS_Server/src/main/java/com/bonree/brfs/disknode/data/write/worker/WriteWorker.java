package com.bonree.brfs.disknode.data.write.worker;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责写数据到文件的Worker类，一个worker对应一个线程。
 * 
 * @author yupeng
 *
 */
public class WriteWorker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(WriteWorker.class);
	
	private LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>(128);
	
	private volatile boolean isQuit = false;
	
	private static AtomicInteger idBuilder = new AtomicInteger(0);
	private final int id;
	
	public WriteWorker() {
		this.id = idBuilder.getAndIncrement();
	}
	
	public <R> void put(WriteTask<R> task) {
		try {
			taskQueue.put(task);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void quit() {
		isQuit = true;
	}

	@Override
	public void run() {
		LOG.info("Woker[{}] started.", id);
		while(!isQuit) {
			Runnable task = null;
			try {
				task = taskQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if(task == null) {
				continue;
			}
			
			try {
				task.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		LOG.info("Woker[{}] quit.", id);
	}
	
	@Override
	public String toString() {
		return WriteWorker.class.getSimpleName() + "#" + id;
	}
}
