package com.br.disknode.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class PooledThreadFactory implements ThreadFactory {
	private AtomicInteger threadId = new AtomicInteger(0);
	
	private String poolName;

	public PooledThreadFactory(String poolName) {
		this.poolName = poolName;
	}
	
	private String nextThreadName() {
		return new StringBuilder()
		          .append(poolName)
		          .append("_")
		          .append(threadId.getAndIncrement())
		          .toString();
	}
	
	@Override
	public Thread newThread(Runnable r) {
		return new Thread(r, nextThreadName());
	}

}
