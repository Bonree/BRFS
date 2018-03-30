package com.bonree.brfs.common.asynctask;

import java.util.concurrent.Executors;

import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 异步任务的执行线程池
 * 
 * @author chen
 *
 */
public class AsyncExecutor {
	private ListeningExecutorService threadPool;
	
	/**
	 * 
	 * @param threadNum 线程池的线程数
	 */
	public AsyncExecutor(int threadNum) {
		threadPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadNum));
	}
	
	/**
	 * 提交一组异步任务，通过Callback的方式回收执行结果，所有任务完成时才会调用callback
	 * 
	 * @param group 任务组
	 * @param callback
	 */
	public <V> void submit(AsyncTaskGroup<V> group, AsyncTaskGroupCallback<V> callback) {
		AsyncTaskResultGather<V> gather = new AsyncTaskResultGather<V>(group.size(), callback);
		for(AsyncTask<V> task : group.getTaskList()) {
			ListenableFuture<AsyncTaskResult<V>> future = threadPool.submit(task);
			Futures.addCallback(future, gather, ThreadPoolUtil.commonPool());
		}
		
	}
}
