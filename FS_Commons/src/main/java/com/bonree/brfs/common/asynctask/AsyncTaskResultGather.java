package com.bonree.brfs.common.asynctask;

import com.google.common.util.concurrent.FutureCallback;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务组中任务执行结果的收集类
 *
 * @param <V>
 *
 * @author chen
 */
public class AsyncTaskResultGather<V> implements FutureCallback<AsyncTaskResult<V>> {
    /**
     * 完成任务数量统计
     */
    private AtomicInteger taskCount = new AtomicInteger();
    private AtomicInteger resultCount = new AtomicInteger();

    private AsyncTaskGroupCallback<V> callback;
    private AsyncTaskResult<V>[] taskResults;

    @SuppressWarnings("unchecked")
    public AsyncTaskResultGather(int taskCount, AsyncTaskGroupCallback<V> callback) {
        this.callback = callback;
        this.taskResults = (AsyncTaskResult<V>[]) Array.newInstance(AsyncTaskResult.class, taskCount);
    }

    @Override
    public void onSuccess(AsyncTaskResult<V> result) {
        taskResults[taskCount.getAndIncrement()] = result;

        //添加count数是保证callback触发时所有结果都填充到数组中了
        if (resultCount.incrementAndGet() == taskResults.length) {
            callback.completed(taskResults);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        //Nothing to do
        t.printStackTrace();
    }

}
