package com.bonree.brfs.common.asynctask;

import java.util.concurrent.Callable;

/**
 * 异步任务抽象类
 * 
 * 每个任务都需要一个任务ID
 * 
 * 具体的任务执行流程实现run方法
 * 
 * ***这只是一种伪异步的实现方式，run方法中的耗时操作依旧会导致线程的阻塞***
 * 
 * @author chen
 *
 * @param <V>
 */
public abstract class AsyncTask<V> implements Callable<AsyncTaskResult<V>> {

	@Override
	public AsyncTaskResult<V> call() throws Exception {
		AsyncTaskResult<V> taskResult = new AsyncTaskResult<V>();
		taskResult.setTaskId(getTaskId());
		
		V result = null;
		try {
			result = run();
			taskResult.setResult(result);
		} catch (Exception e) {
			taskResult.setError(e);
		}
		
		return taskResult;
	}
	
	/**
	 * 获取任务ID
	 * @return
	 */
	public abstract String getTaskId();
	
	/**
	 * 任务内容
	 * @return
	 * @throws Exception
	 */
	public abstract V run() throws Exception;
}
