package com.bonree.brfs.common.asynctask;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.FutureCallback;

/**
 * 任务组中任务执行结果的收集类
 * 
 * @author chen
 *
 * @param <V>
 */
public class AsyncTaskResultGather<V> implements FutureCallback<AsyncTaskResult<V>> {
	/**
	 * 完成任务数量统计
	 */
	private AtomicInteger taskCount = new AtomicInteger();
	
	private AsyncTaskGroupCallback<V> callback;
	private AsyncTaskResult<V>[] taskResults;
	
	@SuppressWarnings("unchecked")
	public AsyncTaskResultGather(int taskCount, AsyncTaskGroupCallback<V> callback) {
		this.callback = callback;
		this.taskResults = (AsyncTaskResult<V>[]) Array.newInstance(AsyncTaskResult.class, taskCount);
	}

	@Override
	public void onSuccess(AsyncTaskResult<V> result) {
		if(result == null) {
			System.out.println("AsyncTaskResultGather--- get null!!!");
		}
		
		int index = taskCount.getAndIncrement();
		taskResults[index] = result;
		
		if((index + 1) == taskResults.length) {
			callback.completed(taskResults);
		}
	}

	@Override
	public void onFailure(Throwable t) {
		//Nothing to do
		System.out.println("AsyncTaskResultGather--- get trouble!!!");
		t.printStackTrace();
	}

}
