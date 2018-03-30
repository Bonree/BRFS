package com.bonree.brfs.common.asynctask;

/**
 * 任务的执行结果
 * 
 * @author chen
 *
 * @param <V>
 */
public class AsyncTaskResult<V> {
	//任务ID
	private String taskId;
	//执行结果
	private V result;
	//执行异常
	private Throwable error;

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public V getResult() {
		return result;
	}

	public void setResult(V result) {
		this.result = result;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}
}
