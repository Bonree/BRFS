package com.bonree.brfs.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 * 
 * @author chen
 *
 */
public final class ThreadPoolUtil {
	private static final int DEFAULT_COMMON_POOL_SIZE = 5;
	private static final String DEFAULT_COMMON_POOL_NAME = "common_pool";
	private static ExecutorService commonPool;
	
	private static final int DEFAULT_SCHEDULE_POOL_SIZE = 3;
	private static final String DEFAULT_SCHEDULE_POOL_NAME = "schedule_pool";
	private static ScheduledExecutorService commonScheduledPool;
	
	static {
		commonPool = new ThreadPoolExecutor(DEFAULT_COMMON_POOL_SIZE,
				DEFAULT_COMMON_POOL_SIZE,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory(DEFAULT_COMMON_POOL_NAME));
		
		commonScheduledPool = new ScheduledThreadPoolExecutor(DEFAULT_SCHEDULE_POOL_SIZE,
				new PooledThreadFactory(DEFAULT_SCHEDULE_POOL_NAME));
	}
	
	public static ExecutorService commonPool() {
		return commonPool;
	}
	
	public static ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
		return commonScheduledPool.schedule(task, delay, unit);
	}
	
	public static ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return commonScheduledPool.scheduleAtFixedRate(command, initialDelay, period, unit);
	}
	
	private ThreadPoolUtil() {}
}
