package com.bonree.brfs.common.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
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
	
	static {
		commonPool = new ThreadPoolExecutor(DEFAULT_COMMON_POOL_SIZE,
				DEFAULT_COMMON_POOL_SIZE,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(DEFAULT_COMMON_POOL_SIZE),
                new PooledThreadFactory(DEFAULT_COMMON_POOL_NAME));
	}
	
	public static ExecutorService commonPool() {
		return commonPool;
	}
	
	private ThreadPoolUtil() {}
}
