package com.bonree.brfs.common.asynctask;

import java.lang.reflect.Array;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.bonree.brfs.common.utils.ThreadPoolUtil;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 异步任务的执行线程池
 * 
 * @author yupeng
 *
 */
public class AsyncExecutor {
	private ListeningExecutorService workerThreadPool;
	
	/**
	 * 
	 * @param threadNum 线程池的线程数
	 */
	public AsyncExecutor(int threadNum, ThreadFactory factory) {
		workerThreadPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadNum, factory));
	}
	
	/**
	 * 提交一组异步任务，通过Callback的方式回收执行结果，所有任务完成时才会调用callback
	 * 
	 * @param group 任务组
	 * @param callback 任务组运行完毕后触发的回调接口
	 * @param executor 运行callback的线程池
	 */
	public <V> void submit(AsyncTaskGroup<V> group, AsyncTaskGroupCallback<V> callback, ExecutorService executor) {
		if(group.isEmpty()) {
			executor.submit(new Runnable() {
				
				@SuppressWarnings("unchecked")
				@Override
				public void run() {
					callback.completed((AsyncTaskResult<V>[]) Array.newInstance(AsyncTaskResult.class, 0));
				}
			});
			
			return;
		}
		
		AsyncTaskResultGather<V> gather = new AsyncTaskResultGather<V>(group.size(), callback);
		for(AsyncTask<V> task : group.getTaskList()) {
			ListenableFuture<AsyncTaskResult<V>> future = workerThreadPool.submit(task);
			Futures.addCallback(future, gather, executor);
		}
	}
}
